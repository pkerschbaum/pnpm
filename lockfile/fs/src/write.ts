import { promises as fs } from 'fs'
import path from 'path'
import { type LockfileFileV9, type Lockfile, type LockfileFile, type ProjectId } from '@pnpm/lockfile.types'
import { WANTED_LOCKFILE } from '@pnpm/constants'
import rimraf from '@zkochan/rimraf'
import yaml from 'js-yaml'
import isEmpty from 'ramda/src/isEmpty'
import writeFileAtomicCB from 'write-file-atomic'
import { lockfileLogger as logger } from './logger'
import { sortLockfileKeys } from './sortLockfileKeys'
import { getWantedLockfileName } from './lockfileName'
import { convertToLockfileFile } from './lockfileFormatConverters'
import { filterLockfileByImporters } from '@pnpm/lockfile.filtering'

async function writeFileAtomic (filename: string, data: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    writeFileAtomicCB(filename, data, {}, (err?: Error) => {
      (err != null) ? reject(err) : resolve()
    })
  })
}

const LOCKFILE_YAML_FORMAT = {
  blankLines: true,
  lineWidth: -1, // This is setting line width to never wrap
  noCompatMode: true,
  noRefs: true,
  sortKeys: false,
}

export async function writeWantedLockfile (
  pkgPath: string,
  wantedLockfile: Lockfile,
  opts?: {
    useGitBranchLockfile?: boolean
    mergeGitBranchLockfiles?: boolean
    lockfilePerWorkspacePackage?: boolean
  }
): Promise<void> {
  const wantedLockfileName: string = await getWantedLockfileName(opts)
  return writeLockfile(wantedLockfileName, pkgPath, wantedLockfile, { lockfilePerWorkspacePackage: opts?.lockfilePerWorkspacePackage })
}

export async function writeCurrentLockfile (
  virtualStoreDir: string,
  currentLockfile: Lockfile
): Promise<void> {
  // empty lockfile is not saved
  if (isEmptyLockfile(currentLockfile)) {
    await rimraf(path.join(virtualStoreDir, 'lock.yaml'))
    return
  }
  await fs.mkdir(virtualStoreDir, { recursive: true })
  return writeLockfile('lock.yaml', virtualStoreDir, currentLockfile, { lockfilePerWorkspacePackage: false })
}

async function writeLockfile (
  lockfileFilename: string,
  pkgPath: string,
  wantedLockfile: Lockfile,
  opts: {
    lockfilePerWorkspacePackage?: boolean
  }
): Promise<void> {
  let lockfilesToWrite: Array<{ lockfileDir: string, lockfile: Lockfile }> = []

  if (!opts.lockfilePerWorkspacePackage) {
    lockfilesToWrite.push({ lockfileDir: pkgPath, lockfile: wantedLockfile })
  } else {
    lockfilesToWrite = computeLockfilesToWrite({ wantedLockfileDir: pkgPath, completeLockfile: wantedLockfile })
  }

  await Promise.all(
    lockfilesToWrite.map(
      async lockfile => {
        const lockfilePath = path.join(lockfile.lockfileDir, lockfileFilename)
        const lockfileToStringify = convertToLockfileFile(lockfile.lockfile, { forceSharedFormat: true })
        const yamlDoc = yamlStringify(lockfileToStringify)
        return writeFileAtomic(lockfilePath, yamlDoc)
      }
    )
  )
}

function yamlStringify (lockfile: LockfileFile) {
  const sortedLockfile = sortLockfileKeys(lockfile as LockfileFileV9)
  return yaml.dump(sortedLockfile, LOCKFILE_YAML_FORMAT)
}

export function isEmptyLockfile (lockfile: Lockfile): boolean {
  return Object.values(lockfile.importers).every((importer) => isEmpty(importer.specifiers ?? {}) && isEmpty(importer.dependencies ?? {}))
}

export async function writeLockfiles (
  opts: {
    wantedLockfile: Lockfile
    wantedLockfileDir: string
    currentLockfile: Lockfile
    currentLockfileDir: string
    useGitBranchLockfile?: boolean
    mergeGitBranchLockfiles?: boolean
    lockfilePerWorkspacePackage?: boolean
  }
): Promise<void> {
  const wantedLockfileName: string = await getWantedLockfileName(opts)
  const currentLockfilePath = path.join(opts.currentLockfileDir, 'lock.yaml')

  const normalizeOpts = {
    forceSharedFormat: true,
  }

  const currentLockfileToStringify = convertToLockfileFile(opts.currentLockfile, normalizeOpts)
  const currentYamlDoc = yamlStringify(currentLockfileToStringify)

  if (!opts.lockfilePerWorkspacePackage) {
    const wantedLockfilePath = path.join(opts.wantedLockfileDir, wantedLockfileName)

    // in most cases the `pnpm-lock.yaml` and `node_modules/.pnpm-lock.yaml` are equal
    // in those cases the YAML document can be stringified only once for both files
    // which is more efficient
    if (opts.wantedLockfile === opts.currentLockfile) {
      await Promise.all([
        writeFileAtomic(wantedLockfilePath, currentYamlDoc),
        (async () => {
          if (isEmptyLockfile(opts.wantedLockfile)) {
            await rimraf(currentLockfilePath)
          } else {
            await fs.mkdir(path.dirname(currentLockfilePath), { recursive: true })
            await writeFileAtomic(currentLockfilePath, currentYamlDoc)
          }
        })(),
      ])
      return
    }

    logger.debug({
      message: `\`${WANTED_LOCKFILE}\` differs from \`${path.relative(opts.wantedLockfileDir, currentLockfilePath)}\``,
      prefix: opts.wantedLockfileDir,
    })

    const wantedLockfileToStringify = convertToLockfileFile(opts.wantedLockfile, normalizeOpts)
    const wantedYamlDoc = yamlStringify(wantedLockfileToStringify)

    await Promise.all([
      writeFileAtomic(wantedLockfilePath, wantedYamlDoc),
      (async () => {
        if (isEmptyLockfile(opts.wantedLockfile)) {
          await rimraf(currentLockfilePath)
        } else {
          await fs.mkdir(path.dirname(currentLockfilePath), { recursive: true })
          await writeFileAtomic(currentLockfilePath, currentYamlDoc)
        }
      })(),
    ])
  } else {
    const lockfilesToWrite = computeLockfilesToWrite({ wantedLockfileDir: opts.wantedLockfileDir, completeLockfile: opts.wantedLockfile })

    await Promise.all([
      ...lockfilesToWrite.map(
        async lockfile => {
          const lockfilePath = path.join(lockfile.lockfileDir, wantedLockfileName)
          const lockfileToStringify = convertToLockfileFile(lockfile.lockfile, { forceSharedFormat: true })
          const yamlDoc = yamlStringify(lockfileToStringify)
          return writeFileAtomic(lockfilePath, yamlDoc)
        }
      ),
      (async () => {
        if (isEmptyLockfile(opts.wantedLockfile)) {
          await rimraf(currentLockfilePath)
        } else {
          await fs.mkdir(path.dirname(currentLockfilePath), { recursive: true })
          await writeFileAtomic(currentLockfilePath, currentYamlDoc)
        }
      })(),
    ])
  }
}

function computeLockfilesToWrite (
  opts: {
    completeLockfile: Lockfile
    wantedLockfileDir: string
  }
): Array<{ lockfileDir: string, lockfile: Lockfile }> {
  const wantedLockfileForRoot = filterLockfileByImporters(opts.completeLockfile, ['.' as ProjectId], { include: { dependencies: true, devDependencies: true, optionalDependencies: true }, skipped: new Set(), failOnMissingDependencies: true })

  // keep all importers in order to keep references to the workspace projects, but remove all dependency information
  wantedLockfileForRoot.importers = Object.fromEntries(Object.entries(wantedLockfileForRoot.importers).map(([projectId, _snapshot]) => ([projectId as ProjectId, { specifiers: {} }])))

  let projectsLockfiles: Array<{ lockfileDir: string, lockfile: Lockfile }> = []
  if (opts.completeLockfile.importers) {
    const projectIdsExcludingRoot = Object.keys(opts.completeLockfile.importers).filter(projectId => projectId !== '.') as ProjectId[]
    projectsLockfiles = projectIdsExcludingRoot.map(projectId => {
      let wantedLockfileForProject = filterLockfileByImporters(opts.completeLockfile, [projectId], { include: { dependencies: true, devDependencies: true, optionalDependencies: true }, skipped: new Set(), failOnMissingDependencies: true })
      wantedLockfileForProject = {
        ...wantedLockfileForProject,
        importers: Object.fromEntries(Object.entries(wantedLockfileForProject.importers).filter(([projectIdOfEntry]) => projectIdOfEntry === projectId)),
      }

      return {
        lockfileDir: path.join(opts.wantedLockfileDir, projectId),
        lockfile: wantedLockfileForProject,
      }
    })
  }

  return [{ lockfileDir: opts.wantedLockfileDir, lockfile: wantedLockfileForRoot }, ...projectsLockfiles]
}