# Architecture

## Classes

```mermaid
classDiagram
namespace proto {
    class DirEntry {
        Directory entry information, =metadata
        --
        file_name
        mode
        uid
        gid
        mtime
        file_type
    }
    class DirContent {
        Content of one directory
        --
        string rel_path
        DirEntry entries
    }
}
namespace dir_sync {
    class Hasher {
        Dedicated thread running Blake3 hasher
        --
        Input: stream of `PathBuf`
        Output: stream of `PathBuf, Hash`
    }
    class DirWalk {
        Dedicated thread walking a directory
        Output the content of each directory:
        - start from root
        - then sub-directories of the current dir, in ascending file_name
        - discard filtered entries ASAP (avoid entering the directories)
        --
        Input: `PathBuf`
        Output: stream of `proto::DirContent` without hash
    }
    class AddHash {
        Add Hash for all regular files:
        - from a previous scan if available
        - otherwise request Hasher to do the hashing
        --
        Input: stream of `proto::DirContent` without hash + old snap
        Output: stream of `proto::DirContent` with hash
    }
    class DirRemote {
        Wrap SSH instance of dir-sync
        Generate a stream of `proto::DirContent` with hash        
    }
    class DiffDirect {
        Direct comparison of directories - do not use hasher:
        - get directories from all inputs
        - compare entries; if two files may be different - same relative path, same size but different mtime -
        compare their content
        - output identified differences
        --
        Input: multiple streams of `proto::DirContent`, without hash
        Output: stream of delta
    }
    class DiffHash {
        Compare directories using hash:
        - get directories from all inputs
        - compare entries - file size and file hash
        - output identified differences
        --
        Input: multiple streams of `proto::DirContent`, with hash
        Output: stream of delta
    }
    class Sync {
        Try to resolve delta using a previous state of all folders
        - if a delta comes from a change on one or several sources, and unchanged on others
        - then propagate the change
        --
        Input: stream of delta + previous MetadataSnap from all sources
        Output: stream of delta with action
    }
    class OutputStatus {
        Stop on first delta with `EXIT_FAILURE`
        If diff completes without delta, exit with `EXIT_SUCCESS`
        --
        Input: stream of delta
        Output: exit status
    }
    class OutputStdout {
        Display delta to stdout
        --
        Input: stream of delta
    }
    class OutputTui {
        TUI application to display delta, sync actions
        and allow user to manually solve delta
        --
        Input: stream of delta, with or without action
    }
    class ExecSyncActions {
        Execute sync actions, either determined by Sync state, or manually by user
        --
        Input: stream of delta with action
    }
}
```

## Pipeline

Pipeline is built based on the configuration, using the above bricks.

Examples:

- diff status: DirWalk + DiffDirect + OutputStatus
- sync batch: DirWalk + AddHash + DiffHash + Sync + ExecSyncActions
- TUI: DirWalk + AddHash + DiffHash + Sync + OutputTui + ExecSyncActions

## MetadataSnap

All metadata of the directory and its content

- name
- type
- regular: size + hash (if size > 0)
- directory: sorted list of entries
- symlink: content (=target)
- ctime
- mtime
- uid/gid (number)
- permissions

## Hasher

Use Blake3, much faster than others.

After benchmark, a single worker thread calling `update_mmap_rayon()` is the most efficient for all use cases.

## DirWalk

Dedicated thread walking a directory.

Input: path to directory
Output: Stream of `proto::DirContent`

## Direct comparison

When all `DirWalk` are local, hasher is not needed. In this case, a direct comparison can be made for regular files with same relative paths, same size but different mtime to check if the files are really different.

## Directory walking stage

Input: path to directory + last known MetadataSnap
Output: MetadataSnap of this directory (and content)

- one thread walking the directory: DirWalk
- single Hasher, shared between local DirWalk
- one task aggregating data: DirStat
  - data from DirWalk
  - compare with last known MetadataSnap
  - request hash to Hasher
  - collect hash results
  - send data to consumer

```mermaid
sequenceDiagram
    actor Input
    participant DirStat@{ "type" : "control" }
    participant DirWalk@{ "type" : "control" }
    participant Hasher@{ "type" : "control" }
    actor Output

    Input->>+Hasher: init
    Input->>+DirStat: init(path, lastMetadataSnap)
    DirStat->>+DirWalk: init(path)

    loop walk directory
        DirWalk-->>DirStat: directory content
        note over DirStat: compare with lastMetadataSnap
        loop new/modified files
            DirStat-->>Hasher: hash(path, size)
        end
    end

    loop hash result
        Hasher-->>DirStat: result(path, hash)
    end

    loop dirstat result
        DirStat-->>Output: result(dir)
    end

    DirWalk-->>-DirStat: end
    DirStat-->>-Output: end

    deactivate Hasher
```

## Draft

- un thread qui parcourt un répertoire et envoie le contenu (fichier ? dossier ?)
- un thread/tache qui collecte le résultat
- consommateur ? fait le diff entre plusieurs répertoires (2 répertoires différents, ou même répertoire comparé avec un MetadataSnap passé)
  - demande un b3sum si besoin
  - reporte les différences

A la fin, on veut:

- vue de toute l'arborescence globale/unifiée, avec les points communs / delta
- arborescence limitée aux deltas/conflits
- pour chaque conflit, action automatique ou manuelle
- exécution des actions
- arbre résultant pour chaque source (on ne refait pas une passe)

Il faut un 1er étage:

- walk dir
- comparaison avec le dernier MetadataSnap connu
- b3sum des fichiers nouveaux ou modifiés
- sortie: MetadataSnap "current"

Ce même étage est fait en local ou à distance; dans les 2 cas; la sortie est le MetadataSnap (complet ou idéalement progressif)

```mermaid
sequenceDiagram
    participant DirWalk1@{ "type" : "control" }
    participant DirWalk2@{ "type" : "control" }
    participant B3sum@{ "type" : "control" }
    participant Main
    participant db@{ "type" : "database" }

    activate Main
    Main->>+DirWalk1: init
    Main->>+DirWalk2: init
    
    loop Until end of walk
        opt DirWalk1 new entry
            DirWalk1-->>Main: dirX
            Main->>db: dirX
        end
        opt DirWalk2 new entry
            DirWalk2-->>Main: dirY
            Main->>db: dirY
        end

    end

    DirWalk1-->>-Main: end
    DirWalk2-->>-Main: end

    deactivate Main
```
