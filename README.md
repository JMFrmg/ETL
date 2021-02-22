# Projet d'ETL
Projet de clôture de la partie data de la formation Simplon IA.
Technos : SQL - BDD Oracle - Python Dash
L'application web se connecte à une base de donnée Oracle (les informations de connexion sont à rentrer dans le fichier main.py lihne 19). Elle récupère les métadonnées de la base (noms des tables, des colonnes, type de données et relation entre les tables).
Elle affiche une représentation graphique de la BDD. L'utilisateur peut a alors sélectionner des tables en fonction des relations existantes. Une fois que l'utilisateur a sélectionner les tables qui l'intéresse, il peut en sélectionner les colonnes. Enfin, la requête SQL correspondante est générée, exécutée et téléchargeable au format texte.
