# SPIREC — Serveur de collecte & analyse (PADRE)

Ce dossier contient le **volet serveur** du projet SPIREC : collecte des
métriques envoyées par des sondes IoT (ESP32, réelles ou simulées) exécutant
l'algorithme de décision d'émission **PADRE**, puis génération des graphiques
d'évaluation (G1 à G6) à partir des données collectées.

PADRE décide, côté sonde, *quand* transmettre une mesure (changements
significatifs, événements critiques, mode survie sur batterie faible, etc.)
plutôt que d'émettre en continu. Le rôle du serveur est de recevoir ces
trames, les stocker/afficher, puis de mesurer l'efficacité du filtrage
(taux de suppression de trafic, réactivité, couplage énergie/activité...).

## Contenu du dossier

| Fichier | Rôle |
|---|---|
| `server.py` | Serveur TCP de supervision **temps réel** (affichage console détaillé, détection d'anomalies, alertes). Usage manuel / debug avec un ou quelques noeuds. |
| `server_sim.py` | Serveur TCP de **collecte pour campagne de simulation** (jusqu'à 5 équipements en parallèle). N'affiche qu'un résumé ligne par ligne et enregistre chaque trame dans un CSV. |
| `analyse_simulation.ipynb` | Notebook générant les figures d'évaluation **G1 à G6** à partir du CSV produit par `server_sim.py` et des traces d'activité. |
| `G1_par_profil.py` | Script autonome qui régénère, pour chaque profil simulé, les graphiques G1 (vue complète + zoom 50 min) « activité réelle vs mesures transmises ». |
| `simulation_3h.csv` | Exemple de sortie de `server_sim.py` (campagne de ~3h, 5 profils). |
| `traces_preview.csv` | Traces d'activité CPU simulée par profil (une colonne par profil), utilisées pour reconstruire l'activité "réelle" entre deux trames reçues. |
| `G1_*.png`, `G2_*.png`, ... `G6_*.png` | Figures déjà générées (résultats de la campagne incluse). |

## Protocole réseau

Les sondes (ou le simulateur) envoient des trames binaires en TCP :

```
[ len_hi ][ len_lo ][ ... enregistrement CBOR ... ]
```

- Longueur sur 2 octets big-endian, suivie du payload encodé en **CBOR**
  (mêmes clés que l'ancien format JSON : `device`, `node_id`, `seq`,
  `cpu_usage_pct`, `battery_mv`, `signal_dbm`, `status`, ...).
- Le serveur répond par un accusé de réception minimal `b"OK"` (utilisé côté
  sonde pour calculer `tx_success_rate` et `consecutive_failures`).
- Le serveur est **passif** : il ne renvoie aucun ordre de reconfiguration,
  le rythme d'émission est entièrement piloté par PADRE côté noeud.

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install cbor2 pandas numpy matplotlib jupyter
```

(un environnement `.venv` est déjà présent dans ce dossier avec ces
dépendances installées)

## Utilisation

### 1. Supervision temps réel d'un ou plusieurs noeuds

```bash
python3 server.py
```

Écoute sur `0.0.0.0:8080`, affiche pour chaque trame reçue un tableau de
bord détaillé (CPU, mémoire, réseau, qualité de transmission, énergie) avec
détection d'anomalies (perte de paquets, reboot, spike TX...) et un résumé
de session à l'arrêt (Ctrl+C).

### 2. Campagne de simulation (collecte multi-équipements)

```bash
python3 server_sim.py --port 8080 --csv simulation_3h.csv
```

Accepte plusieurs connexions simultanées (un thread par équipement),
enregistre **chaque** trame reçue dans le CSV (avec horodatage serveur) sans
rien filtrer côté serveur — c'est PADRE, côté sonde, qui a déjà décidé quoi
transmettre.

### 3. Génération des graphiques d'évaluation

Ouvrir et exécuter `analyse_simulation.ipynb` (nécessite `simulation_3h.csv`
et `traces_preview.csv` dans le même dossier), ou régénérer uniquement les
figures G1 par profil :

```bash
python3 G1_par_profil.py
```

| Figure | Ce qu'elle montre |
|---|---|
| G1 | Signal réel vs mesures transmises (filtrage PADRE), par profil |
| G2 | Taux de suppression de trafic par profil (adaptativité) |
| G3 | Décharge batterie des 5 équipements (couplage énergie-activité) |
| G4 | Mode survie : émissions ralenties → chute de batterie freinée |
| G5 | Réactivité à l'événement critique (décision lexicographique) |
| G6 | Trafic PADRE vs transmission systématique (gain pour l'edge) |

## Résultats (campagne incluse)

Sur la campagne de simulation fournie (~3h, 5 profils) : réduction globale du
trafic d'environ **84,5 %**, taux de suppression allant de **79 %** (profil
critique) à **93 %** (profil batterie faible), et mode survie effectif sur
l'équipement en fin de batterie.
