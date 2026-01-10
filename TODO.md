# TODO

`ssh machine -e none` pour avoir un channel transparent

## clap

## proto tree

## SSH

## WIP

Rework DirStat
1 seul diff/sync engine, qui a aussi la responsabilité d'écrire le DirStat ?

Ou alors le dirstat est passé vers l'output à la fin ?
=> voir combien de temps prend un dirstat la première fois (5 min sur /data a priori) vs la 2e fois (dir walk only, mtime + size correspondent donc pas de hash)
4m33 vs moins d'une sec (0.65s et même 0.20s quand les metadata sont en cache)

=> pas besoin de pipeliner à outrance ?
=> plus facile de faire le diff en 1 fois sans pipeline ?
=> plus besoin de pipeline
 => on lance dir_walk + dir_stat (local/remote)
 => on récupère le résultat
 => diff
 => sync
 => traitement

## Roadmap

- étage de diff
- implémenter status
- à tester avec rootfs
- à deployer dans clonux ?
- implémenter diff output
- réutiliser pour faire un pretty print de DumpMetadataSnap ?
- support remote
- ajout sync batch local only
- ajout sync batch remote
- TUI
- déploiement dans clonux, suppression complète d'unison (install, etc, var local, UNISON env var)
