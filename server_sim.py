#!/usr/bin/env python3
"""
server_sim.py — Serveur de collecte pour la CAMPAGNE DE SIMULATION SPIREC.

Recoit les trames CBOR des equipements simules (jusqu'a 5 en parallele) et les
ENREGISTRE dans un fichier CSV, pour generer les graphiques a posteriori
(G1-G6). Ne surveille rien en direct : il collecte, horodate et stocke.

Chaque equipement se presente sous un nom distinct (sim-<profil>) et emet des
trames CBOR selon la decision de PADRE. Le serveur enregistre CHAQUE trame recue
avec son horodatage serveur, permettant ensuite de :
  - tracer signal reel vs mesures transmises (G1) ;
  - deduire le taux de suppression des sauts de seq (G2, G6) ;
  - suivre la decharge batterie et le mode survie (G3, G4) ;
  - reperer la reaction aux evenements critiques (G5).

Protocole (identique au firmware) :
  trame = [ len(2 octets, big-endian) ][ enregistrement CBOR ]
  ACK   = b"OK" (accuse simple ; l'edge ne reconfigure rien)

Usage :
    python3 server_sim.py [--port 8080] [--csv resultats_sim.csv]

Prerequis : pip install cbor2
"""

import argparse
import csv
import socket
import struct
import threading
import time
from datetime import datetime, timezone

try:
    import cbor2
except ImportError:
    raise SystemExit("Dependance manquante : pip install cbor2")

# ---------------------------------------------------------------------------
# Colonnes du CSV. On enregistre l'horodatage serveur + l'IP source + TOUTES
# les metriques de la trame (pour ne rien perdre). L'ordre est fixe ; les
# metriques absentes d'une trame sont laissees vides.
# ---------------------------------------------------------------------------
CHAMPS_META = ["ts_serveur", "ts_epoch", "ip_source"]
CHAMPS_TRAME = [
    "device", "type", "os", "transport", "node_id",
    "seq", "uptime_ms",
    "cpu_usage_pct", "free_heap_bytes", "stack_usage_pct", "active_threads",
    "battery_mv", "energy_proxy", "sleep_ratio_pct",
    "signal_dbm", "last_send_duration_ms",
    "bytes_tx", "bytes_rx", "transport_errors", "coap_retransmissions",
    "tx_success_rate", "consecutive_failures",
    "reset_count", "update_count", "status",
]
COLONNES = CHAMPS_META + CHAMPS_TRAME


class Enregistreur:
    """Ecriture CSV thread-safe (plusieurs equipements ecrivent en parallele)."""

    def __init__(self, chemin):
        self.chemin = chemin
        self._lock = threading.Lock()
        self._f = open(chemin, "w", newline="", encoding="utf-8")
        self._w = csv.DictWriter(self._f, fieldnames=COLONNES,
                                 extrasaction="ignore")
        self._w.writeheader()
        self._f.flush()
        self.n = 0

    def ecrire(self, meta, trame):
        ligne = dict(meta)
        for k in CHAMPS_TRAME:
            ligne[k] = trame.get(k, "")
        with self._lock:
            self._w.writerow(ligne)
            self._f.flush()      # flush a chaque trame : rien perdu si arret
            self.n += 1

    def fermer(self):
        with self._lock:
            self._f.close()


def lire_trame(conn):
    """Lit une trame [len(2, BE)][CBOR]. Retourne le dict, ou None si ferme."""
    # 1) longueur sur 2 octets big-endian
    hdr = b""
    while len(hdr) < 2:
        chunk = conn.recv(2 - len(hdr))
        if not chunk:
            return None
        hdr += chunk
    (clen,) = struct.unpack(">H", hdr)
    if clen == 0 or clen > 4096:
        return None
    # 2) charge utile CBOR
    buf = b""
    while len(buf) < clen:
        chunk = conn.recv(clen - len(buf))
        if not chunk:
            return None
        buf += chunk
    try:
        return cbor2.loads(buf)
    except Exception:
        return None


def gerer_client(conn, addr, enr, compteurs):
    ip = addr[0]
    print(f"[+] connexion de {ip}")
    try:
        while True:
            trame = lire_trame(conn)
            if trame is None:
                break

            now = datetime.now(timezone.utc)
            meta = {
                "ts_serveur": now.isoformat(),
                "ts_epoch": now.timestamp(),
                "ip_source": ip,
            }
            enr.ecrire(meta, trame)

            dev = trame.get("device", "?")
            seq = trame.get("seq", "?")
            cpu = trame.get("cpu_usage_pct", "?")
            batt = trame.get("battery_mv", "?")
            st = trame.get("status", "?")
            compteurs[dev] = compteurs.get(dev, 0) + 1
            print(f"    {dev:22s} seq={seq:<5} cpu={cpu:<3} "
                  f"batt={batt:<5}mV {st:<12} (#{compteurs[dev]})")

            # ACK simple (indispensable pour tx_success_rate cote sonde)
            conn.sendall(b"OK")
    except (ConnectionResetError, OSError):
        pass
    finally:
        conn.close()
        print(f"[-] deconnexion de {ip}")


def main():
    ap = argparse.ArgumentParser(description="Serveur de collecte (simulation).")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--csv", default=None,
                    help="fichier CSV de sortie (defaut : horodate)")
    args = ap.parse_args()

    csv_path = args.csv or f"resultats_sim_{int(time.time())}.csv"
    enr = Enregistreur(csv_path)
    compteurs = {}

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", args.port))
    srv.listen(8)

    print("=" * 62)
    print(" Serveur de collecte — CAMPAGNE DE SIMULATION SPIREC")
    print(f"   Ecoute      : 0.0.0.0:{args.port}")
    print(f"   Enregistre  : {csv_path}")
    print(f"   Trame       : [len(2)][CBOR]  — ACK : OK")
    print("   Ctrl+C pour arreter (le CSV est flush a chaque trame)")
    print("=" * 62)

    try:
        while True:
            conn, addr = srv.accept()
            t = threading.Thread(target=gerer_client,
                                 args=(conn, addr, enr, compteurs),
                                 daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n[arret] fermeture...")
    finally:
        srv.close()
        enr.fermer()
        total = sum(compteurs.values())
        print(f"[bilan] {total} trames enregistrees dans {csv_path}")
        for dev, n in sorted(compteurs.items()):
            print(f"    {dev:22s} {n} trames")


if __name__ == "__main__":
    main()
