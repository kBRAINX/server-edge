#!/usr/bin/env python3
"""
server.py — v3 (CBOR)
---------------------
Serveur TCP pour la collecte des metriques IoT.

CHANGEMENTS v2 -> v3
  - Transport applicatif : fini HTTP/JSON. Chaque noeud (Wi-Fi direct ou BLE
    via la passerelle) envoie une TRAME BINAIRE :

        [ len_hi ][ len_lo ][ ... CBOR ... ]

    soit une longueur sur 2 octets (big-endian) suivie de l'enregistrement
    CBOR. Le prefixe de longueur remplace le delimiteur '\\n' (impossible avec
    du binaire). cbor2.loads() rend un dict AUX MEMES CLES que l'ancien JSON :
    toute la logique metier (status, deltas, anomalies, reconfiguration) est
    donc conservee telle quelle.
  - Jeu de metriques mis a jour :
        + node_id, battery_mv, energy_proxy, coap_retransmissions, update_count
        - idle_ratio_pct (recalcule ici : 100 - cpu), timestamp_ms (remplace
          par uptime_ms), rssi_dbm (-> signal_dbm), net_errors, tcp_retransmissions
  - ACK : simple accuse de reception "OK" (plus d'enveloppe HTTP).

MISE A JOUR (edge passif) :
  - SUPPRESSION de la reconfiguration d'intervalle. Le serveur (edge) ne renvoie
    plus d'ordre "INTERVAL=<n>" : le rythme de collecte est gere exclusivement
    par l'algorithme PADRE embarque sur le noeud. Le serveur se limite a
    collecter, reconstruire (maintien d'ordre zero via l'historique) et analyser.
  - L'ACK est reduit a "OK" : simple accuse de reception, conserve car il
    alimente tx_success_rate et consecutive_failures cote sonde.

Dependance : pip install cbor2

Utilisation :
    python3 server.py
"""

import socket
import datetime
import sys

try:
    import cbor2
except ImportError:
    sys.exit("Dependance manquante : pip install cbor2")

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
SERVER_IP   = "0.0.0.0"
SERVER_PORT = 8080
BACKLOG     = 5
FRAME_MAX   = 4096  # garde-fou taille CBOR

# Seuils d'alerte — coherents avec derive_status() cote Rust
SEUIL_CPU_PCT          = 80
SEUIL_HEAP_BYTES       = 4096
SEUIL_STACK_PCT        = 85
SEUIL_SIGNAL_DBM       = -80
SEUIL_TX_SUCCESS_WARN  = 90
SEUIL_TX_SUCCESS_CRIT  = 70
SEUIL_CONSEC_FAILURES  = 3
SEUIL_SEND_DURATION_MS = 2000
SEUIL_BATTERY_MV       = 3300   # coupure Li-ion (0 = non mesuree)

HISTORIQUE_MAX = 50
historique: dict[str, list] = {}


# ------------------------------------------------------------------
# Utilitaires d'affichage
# ------------------------------------------------------------------

def ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]


def barre(valeur: int, maxi: int = 100, largeur: int = 20) -> str:
    valeur = max(0, min(int(valeur), maxi))
    fill = int(largeur * valeur / maxi)
    return f"[{'█' * fill}{'░' * (largeur - fill)}] {valeur:3d}%"


def alerte(condition: bool, msg: str = "⚠ ALERTE") -> str:
    return f"  {msg}" if condition else ""


def delta_str(actuel, precedent, unite: str = "") -> str:
    if precedent is None:
        return ""
    diff = actuel - precedent
    signe = "+" if diff >= 0 else ""
    return f"  (Δ {signe}{diff}{unite})"


# ------------------------------------------------------------------
# Lecture d'une trame [len(2, BE)][CBOR]
# ------------------------------------------------------------------

def recv_exact(conn: socket.socket, n: int) -> bytes | None:
    """Lit EXACTEMENT n octets, ou None si la connexion se ferme avant."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return bytes(buf)


def lire_trame(conn: socket.socket) -> bytes | None:
    """Lit une trame complete : 2 octets de longueur puis le CBOR."""
    hdr = recv_exact(conn, 2)
    if hdr is None:
        return None
    size = (hdr[0] << 8) | hdr[1]
    if size == 0 or size > FRAME_MAX:
        print(f"[{ts()}] Trame de taille invalide : {size}")
        return None
    return recv_exact(conn, size)


# ------------------------------------------------------------------
# Detection anomalies inter-sequences
# ------------------------------------------------------------------

def detecter_anomalies(data: dict, precedent: dict | None) -> list[str]:
    """Compare la mesure courante et la precedente : perte de paquets, reboot,
    spike TX, reset compteur. La base de temps est uptime_ms (monotone)."""
    alertes = []
    if precedent is None:
        return alertes

    gap = data.get("seq", 0) - precedent.get("seq", 0)
    if gap > 1:
        alertes.append(f"⚠  PERTE : {gap - 1} paquet(s) manquant(s) "
                       f"(seq {precedent.get('seq', 0)} → {data.get('seq', 0)})")

    uptime_curr = data.get("uptime_ms", 0)
    uptime_prev = precedent.get("uptime_ms", 0)
    if uptime_curr < uptime_prev:
        alertes.append(f"⚠  REBOOT detecte : uptime {uptime_prev} ms → {uptime_curr} ms")

    upd_curr = data.get("update_count", 0)
    upd_prev = precedent.get("update_count", 0)
    if upd_curr > upd_prev:
        alertes.append(f"↻  MISE A JOUR A CHAUD : nouveau module (update_count "
                       f"{upd_prev} → {upd_curr}, sans reboot)")

    tx_curr = data.get("bytes_tx", 0)
    tx_prev = precedent.get("bytes_tx", 0)
    if tx_curr < tx_prev:
        alertes.append(f"⚠  RESET compteur TX : {tx_prev} → {tx_curr} (redemarrage)")
    elif uptime_curr > uptime_prev:
        delta_t_s = (uptime_curr - uptime_prev) / 1000.0
        delta_tx = tx_curr - tx_prev
        if delta_t_s > 0 and (delta_tx / delta_t_s) > 50_000:
            alertes.append(f"⚠  SPIKE TX : {delta_tx / delta_t_s:.0f} B/s sur {delta_t_s:.1f}s")

    return alertes


# ------------------------------------------------------------------
# Affichage des metriques
# ------------------------------------------------------------------

def afficher_metriques(data: dict, source: str):
    device    = data.get("device", "?")
    node_id   = data.get("node_id", 0)
    os_name   = data.get("os", "?")
    dev_type  = data.get("type", "?")
    transport = data.get("transport", "?")
    seq       = data.get("seq", 0)
    status    = data.get("status", "?")

    # Bloc B — CPU (idle recalcule ici : 100 - cpu)
    cpu     = data.get("cpu_usage_pct", 0)
    idle    = max(0, 100 - cpu)
    threads = data.get("active_threads", 0)
    # Bloc C — Memoire
    heap    = data.get("free_heap_bytes", 0)
    stack   = data.get("stack_usage_pct", 0)
    # Bloc D — Disponibilite
    uptime_ms = data.get("uptime_ms", 0)
    resets    = data.get("reset_count", 0)
    updates   = data.get("update_count", 0)
    # Bloc E — Reseau
    bytes_tx = data.get("bytes_tx", 0)
    bytes_rx = data.get("bytes_rx", 0)
    signal   = data.get("signal_dbm", 0)
    errors   = data.get("transport_errors", 0)
    # Bloc F — Qualite de transmission
    tx_ok       = data.get("tx_success_rate", 100)
    retrans     = data.get("coap_retransmissions", 0)
    last_dur    = data.get("last_send_duration_ms", 0)
    consec_fail = data.get("consecutive_failures", 0)
    # Bloc G — Energie
    sleep_pct   = data.get("sleep_ratio_pct", 0)
    battery_mv  = data.get("battery_mv", 0)
    energy_prox = data.get("energy_proxy", 0)

    precedent = historique.get(device, [None])[-1] if historique.get(device) else None
    prev_cpu    = precedent.get("cpu_usage_pct",  None) if precedent else None
    prev_heap   = precedent.get("free_heap_bytes", None) if precedent else None
    prev_tx     = precedent.get("bytes_tx",        None) if precedent else None
    prev_rx     = precedent.get("bytes_rx",        None) if precedent else None
    prev_signal = precedent.get("signal_dbm",      None) if precedent else None
    prev_tx_ok  = precedent.get("tx_success_rate", None) if precedent else None
    prev_upt    = precedent.get("uptime_ms",       None) if precedent else None

    uptime_s = uptime_ms // 1000
    uh, um, us = uptime_s // 3600, (uptime_s % 3600) // 60, uptime_s % 60

    anomalies = detecter_anomalies(data, precedent)

    icone = {"ok": "✓", "cpu_saturated": "✗", "heap_low": "✗",
             "net_degraded": "✗", "stack_overflow_risk": "⚠",
             "link_down": "✗"}.get(status, "?")
    sep = "─" * 60

    print(f"\n╔{'═' * 60}╗")
    print(f"║  [{ts()}]  {device:<12} ({os_name})  seq={seq:<6}         ║")
    print(f"║  node_id : {node_id:<10}  Type : {dev_type:<12}  Tr : {transport:<6}      ║")
    print(f"║  Statut : {icone} {status:<25}  via {source:<10}     ║")
    print(f"╚{'═' * 60}╝")

    for a in anomalies:
        print(f"  {a}")
    if anomalies:
        print(f"  {sep}")

    print(f"  {'MÉTRIQUE':<35} {'VALEUR':>12}  INFO")
    print(f"  {sep}")

    # ── Bloc B — CPU
    print(f"  B1  CPU usage        {barre(cpu)}"
          f"{alerte(cpu > SEUIL_CPU_PCT)}{delta_str(cpu, prev_cpu, '%')}")
    print(f"  B2  Idle ratio       {barre(idle)}")
    print(f"  B3  Threads actifs   {'':>20} {threads:>4}"
          f"{'  ⚠ fuite ?' if threads > 20 else ''}")
    print(f"  {sep}")

    # ── Bloc C — Memoire
    print(f"  C1  Free heap        {heap/1024:>20.1f} KB"
          f"{alerte(heap < SEUIL_HEAP_BYTES)}"
          f"{delta_str(heap, prev_heap, 'B') if prev_heap else ''}")
    print(f"  C2  Stack usage      {barre(stack)}{alerte(stack > SEUIL_STACK_PCT)}")
    print(f"  {sep}")

    # ── Bloc D — Disponibilite
    print(f"  D1  Uptime           {uh:02d}h{um:02d}m{us:02d}s  ({uptime_ms} ms)")
    print(f"  D2  Reset count      {'':>20} {resets:>4}  (reboots materiels)")
    print(f"  D3  Update count     {'':>20} {updates:>4}  (mises a jour a chaud)")
    print(f"  {sep}")

    # ── Bloc E — Reseau (deltas sur base uptime)
    delta_t_s = ((uptime_ms - prev_upt) / 1000.0) if prev_upt else None
    tx_ds = rx_ds = ""
    if prev_tx is not None and delta_t_s and delta_t_s > 0:
        dtx, drx = bytes_tx - prev_tx, bytes_rx - prev_rx
        tx_ds = f"  (+{dtx} B / {dtx/delta_t_s:.0f} B/s)"
        rx_ds = f"  (+{drx} B / {drx/delta_t_s:.0f} B/s)"
    print(f"  E1  Bytes TX         {bytes_tx:>20} B{tx_ds}")
    print(f"  E2  Bytes RX         {bytes_rx:>20} B{rx_ds}")
    print(f"  E3  Signal           {signal:>+20} dBm"
          f"{alerte(signal < SEUIL_SIGNAL_DBM and signal != 0)}"
          f"{delta_str(signal, prev_signal, ' dBm')}")
    print(f"  E4  Transport errors {'':>20} {errors:>4}")
    print(f"  {sep}")

    # ── Bloc F — Qualite de transmission
    print(f"  F1  TX success rate  {barre(tx_ok)}"
          f"{alerte(tx_ok < SEUIL_TX_SUCCESS_WARN, '⚠ DÉGRADÉ')}"
          f"{alerte(tx_ok < SEUIL_TX_SUCCESS_CRIT, '✗ CRITIQUE')}"
          f"{delta_str(tx_ok, prev_tx_ok, '%')}")
    print(f"  F2  CoAP retrans     {'':>20} {retrans:>4}"
          f"{'  ⚠ congestion' if retrans > 5 else ''}")
    print(f"  F3  Durée envoi      {last_dur:>20} ms"
          f"{alerte(last_dur > SEUIL_SEND_DURATION_MS, '⚠ LENT')}")
    print(f"  F4  Échecs consec.   {'':>20} {consec_fail:>4}"
          f"{alerte(consec_fail >= SEUIL_CONSEC_FAILURES, '✗ LINK DOWN')}")
    print(f"  {sep}")

    # ── Bloc G — Energie
    print(f"  G1  Sleep ratio      {barre(sleep_pct)}")
    if battery_mv > 0:
        print(f"  G2  Batterie         {battery_mv:>20} mV"
              f"{alerte(battery_mv < SEUIL_BATTERY_MV, '✗ FAIBLE')}")
    else:
        print(f"  G2  Batterie         {'non mesurée':>20}")
    print(f"  G3  Energy proxy     {energy_prox:>20}  (octets radio cumulés)")
    print(f"  {sep}\n")


# ------------------------------------------------------------------
# Historique
# ------------------------------------------------------------------

def enregistrer(data: dict):
    device = data.get("device", "unknown")
    historique.setdefault(device, []).append(data)
    if len(historique[device]) > HISTORIQUE_MAX:
        historique[device].pop(0)


def afficher_resume():
    if not historique:
        return
    print("\n" + "═" * 60)
    print("  RÉSUMÉ SESSION")
    print("═" * 60)
    for device, entries in historique.items():
        if not entries:
            continue
        nb    = len(entries)
        cpus  = [e.get("cpu_usage_pct", 0) for e in entries]
        sigs  = [e.get("signal_dbm", 0) for e in entries]
        txoks = [e.get("tx_success_rate", 100) for e in entries]
        retrs = [e.get("coap_retransmissions", 0) for e in entries]
        last  = entries[-1]
        pertes = sum(max(0, entries[i]["seq"] - entries[i-1]["seq"] - 1)
                     for i in range(1, len(entries)))
        print(f"  Device   : {device}")
        print(f"  Mesures  : {nb}")
        print(f"  CPU      : moy={sum(cpus)//nb}%  max={max(cpus)}%  min={min(cpus)}%")
        print(f"  Signal   : moy={sum(sigs)//nb} dBm  min={min(sigs)} dBm")
        print(f"  TX OK    : moy={sum(txoks)//nb}%  min={min(txoks)}%")
        print(f"  Retrans  : total={sum(retrs)}")
        print(f"  Pertes   : {pertes} paquet(s) manquant(s)")
        print(f"  Batterie : {last.get('battery_mv', 0)} mV (0 = non mesurée)")
        print(f"  Uptime   : {last.get('uptime_ms', 0)} ms (dernière mesure)")
        print(f"  Status   : {last.get('status', '?')} (dernière mesure)")
        print("─" * 60)


# ------------------------------------------------------------------
# Handler client — une trame [len][CBOR] par connexion
# ------------------------------------------------------------------

def handle_client(conn: socket.socket, addr: tuple):
    conn.settimeout(5.0)
    source = f"{addr[0]}"
    try:
        frame = lire_trame(conn)
        if frame is None:
            return
        try:
            data = cbor2.loads(frame)
        except Exception as e:
            print(f"[{ts()}] ERREUR CBOR : {e}  ({len(frame)} octets)")
            conn.sendall(b"ERR")
            return
        if not isinstance(data, dict):
            print(f"[{ts()}] Trame CBOR inattendue (pas un dict)")
            conn.sendall(b"ERR")
            return

        enregistrer(data)
        afficher_metriques(data, source)

        # ACK minimal : simple accuse de reception. L'edge NE renvoie PLUS
        # d'ordre de reconfiguration (le rythme est gere par PADRE sur le noeud).
        # Cet accuse reste indispensable : il alimente tx_success_rate et
        # consecutive_failures cote sonde.
        conn.sendall(b"OK")

    except socket.timeout:
        print(f"[{ts()}] Timeout lecture depuis {addr[0]}")
    except Exception as e:
        print(f"[{ts()}] Erreur handler : {e}")
    finally:
        conn.close()


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    print("╔" + "═" * 60 + "╗")
    print("║   Serveur TCP — Métriques IoT  v3 (CBOR)                 ║")
    print(f"║   Écoute sur {SERVER_IP}:{SERVER_PORT}                                  ║")
    print("║   Trame : [len(2)][CBOR]  — ACK : OK (accuse simple)     ║")
    print("║   Ctrl+C pour arrêter                                    ║")
    print("╚" + "═" * 60 + "╝\n")

    print("  Seuils d'alerte :")
    print(f"    CPU                > {SEUIL_CPU_PCT}%")
    print(f"    Heap free          < {SEUIL_HEAP_BYTES} B")
    print(f"    Stack              > {SEUIL_STACK_PCT}%")
    print(f"    Signal             < {SEUIL_SIGNAL_DBM} dBm")
    print(f"    TX success (warn)  < {SEUIL_TX_SUCCESS_WARN}%")
    print(f"    TX success (crit)  < {SEUIL_TX_SUCCESS_CRIT}%")
    print(f"    Consec. failures  >= {SEUIL_CONSEC_FAILURES}")
    print(f"    Batterie           < {SEUIL_BATTERY_MV} mV")
    print()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((SERVER_IP, SERVER_PORT))
    server_sock.listen(BACKLOG)

    print(f"En attente de connexions sur le port {SERVER_PORT}...\n")
    try:
        while True:
            conn, addr = server_sock.accept()
            handle_client(conn, addr)
    except KeyboardInterrupt:
        print("\nArrêt du serveur.")
        afficher_resume()
    finally:
        server_sock.close()


if __name__ == "__main__":
    main()
