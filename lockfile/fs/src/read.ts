import assert from 'node:assert'
import { promises as fs } from 'fs'
import path from 'path'
import util from 'util'
import {
  LOCKFILE_VERSION,
  WANTED_LOCKFILE,
} from '@pnpm/constants'
import { PnpmError } from '@pnpm/error'
import { mergeLockfileChanges } from '@pnpm/lockfile.merger'
import { type Lockfile } from '@pnpm/lockfile.types'
import { type ProjectId } from '@pnpm/types'
import comverToSemver from 'comver-to-semver'
import yaml from 'js-yaml'
import semver from 'semver'
import stripBom from 'strip-bom'
import { LockfileBreakingChangeError } from './errors'
import { autofixMergeConflicts, isDiff } from './gitMergeFile'
import { lockfileLogger as logger } from './logger'
import { getWantedLockfileName } from './lockfileName'
import { getGitBranchLockfileNames } from './gitBranchLockfile'
import { convertToLockfileObject } from './lockfileFormatConverters'

export async function readCurrentLockfile (
  virtualStoreDir: string,
  opts: {
    wantedVersions?: string[]
    ignoreIncompatible: boolean
  }
): Promise<Lockfile | null> {
  const lockfilePath = path.join(virtualStoreDir, 'lock.yaml')
  return (await _read(lockfilePath, virtualStoreDir, opts)).lockfile
}

export async function readWantedLockfileAndAutofixConflicts (
  pkgPath: string,
  opts: {
    wantedVersions?: string[]
    ignoreIncompatible: boolean
    useGitBranchLockfile?: boolean
    mergeGitBranchLockfiles?: boolean
  }
): Promise<{
    lockfile: Lockfile | null
    hadConflicts: boolean
  }> {
  const resultForRootImporter = await _readWantedLockfile(pkgPath, {
    ...opts,
    autofixMergeConflicts: true,
  })

  let lockfile = resultForRootImporter.lockfile
  let hadConflicts = resultForRootImporter.hadConflicts
  if (lockfile) {
    const projectIdsExcludingRoot = Object.keys(lockfile.importers).filter(projectId => projectId !== '.') as ProjectId[]
    await Promise.all(projectIdsExcludingRoot.map(async projectId => {
      assert(lockfile)
      const projectPath = path.join(pkgPath, projectId)
      const resultForProjectImporter = await _readWantedLockfile(projectPath, {
        ...opts,
        autofixMergeConflicts: true,
      })
      if (resultForProjectImporter.lockfile) {
        lockfile = mergeLockfileChanges(lockfile, resultForProjectImporter.lockfile)
        hadConflicts = hadConflicts || resultForProjectImporter.hadConflicts
      }
    }))
  }

  return {
    lockfile,
    hadConflicts,
  }
}

export async function readWantedLockfile (
  pkgPath: string,
  opts: {
    wantedVersions?: string[]
    ignoreIncompatible: boolean
    useGitBranchLockfile?: boolean
    mergeGitBranchLockfiles?: boolean
  }
): Promise<Lockfile | null> {
  return (await _readWantedLockfile(pkgPath, opts)).lockfile
}

async function _read (
  lockfilePath: string,
  prefix: string, // only for logging
  opts: {
    autofixMergeConflicts?: boolean
    wantedVersions?: string[]
    ignoreIncompatible: boolean
  }
): Promise<{
    lockfile: Lockfile | null
    hadConflicts: boolean
  }> {
  let lockfileRawContent
  try {
    lockfileRawContent = stripBom(await fs.readFile(lockfilePath, 'utf8'))
  } catch (err: unknown) {
    if (!(util.types.isNativeError(err) && 'code' in err && err.code === 'ENOENT')) {
      throw err
    }
    return {
      lockfile: null,
      hadConflicts: false,
    }
  }
  let lockfile: Lockfile
  let hadConflicts!: boolean
  try {
    lockfile = convertToLockfileObject(yaml.load(lockfileRawContent) as any) // eslint-disable-line
    hadConflicts = false
  } catch (err: unknown) {
    if (!opts.autofixMergeConflicts || !isDiff(lockfileRawContent)) {
      throw new PnpmError('BROKEN_LOCKFILE', `The lockfile at "${lockfilePath}" is broken: ${(err as Error).message}`)
    }
    hadConflicts = true
    lockfile = autofixMergeConflicts(lockfileRawContent)
    logger.info({
      message: `Merge conflict detected in ${WANTED_LOCKFILE} and successfully merged`,
      prefix,
    })
  }
  if (lockfile) {
    const lockfileSemver = comverToSemver((lockfile.lockfileVersion ?? 0).toString())

    if (
      !opts.wantedVersions ||
      opts.wantedVersions.length === 0 ||
      opts.wantedVersions.some((wantedVersion) => {
        if (semver.major(lockfileSemver) !== semver.major(comverToSemver(wantedVersion))) return false
        if (lockfile.lockfileVersion !== '6.1' && semver.gt(lockfileSemver, comverToSemver(wantedVersion))) {
          logger.warn({
            message: `Your ${WANTED_LOCKFILE} was generated by a newer version of pnpm. ` +
              `It is a compatible version but it might get downgraded to version ${wantedVersion}`,
            prefix,
          })
        }
        return true
      })
    ) {
      return { lockfile, hadConflicts }
    }
  }
  if (opts.ignoreIncompatible) {
    logger.warn({
      message: `Ignoring not compatible lockfile at ${lockfilePath}`,
      prefix,
    })
    return { lockfile: null, hadConflicts: false }
  }
  throw new LockfileBreakingChangeError(lockfilePath)
}

export function createLockfileObject (
  importerIds: ProjectId[],
  opts: {
    lockfileVersion: string
    autoInstallPeers: boolean
    excludeLinksFromLockfile: boolean
    peersSuffixMaxLength: number
  }
): Lockfile {
  const importers = importerIds.reduce((acc, importerId) => {
    acc[importerId] = {
      dependencies: {},
      specifiers: {},
    }
    return acc
  }, {} as Lockfile['importers'])
  return {
    importers,
    lockfileVersion: opts.lockfileVersion || LOCKFILE_VERSION,
    settings: {
      autoInstallPeers: opts.autoInstallPeers,
      excludeLinksFromLockfile: opts.excludeLinksFromLockfile,
      peersSuffixMaxLength: opts.peersSuffixMaxLength,
    },
  }
}

async function _readWantedLockfile (
  pkgPath: string,
  opts: {
    wantedVersions?: string[]
    ignoreIncompatible: boolean
    useGitBranchLockfile?: boolean
    mergeGitBranchLockfiles?: boolean
    autofixMergeConflicts?: boolean
  }
): Promise<{
    lockfile: Lockfile | null
    hadConflicts: boolean
  }> {
  const lockfileNames: string[] = [WANTED_LOCKFILE]
  if (opts.useGitBranchLockfile) {
    const gitBranchLockfileName: string = await getWantedLockfileName(opts)
    if (gitBranchLockfileName !== WANTED_LOCKFILE) {
      lockfileNames.unshift(gitBranchLockfileName)
    }
  }
  let result: { lockfile: Lockfile | null, hadConflicts: boolean } = { lockfile: null, hadConflicts: false }
  /* eslint-disable no-await-in-loop */
  for (const lockfileName of lockfileNames) {
    result = await _read(path.join(pkgPath, lockfileName), pkgPath, { ...opts, autofixMergeConflicts: true })
    if (result.lockfile) {
      if (opts.mergeGitBranchLockfiles) {
        result.lockfile = await _mergeGitBranchLockfiles(result.lockfile, pkgPath, pkgPath, opts)
      }
      break
    }
  }
  /* eslint-enable no-await-in-loop */
  return result
}

async function _mergeGitBranchLockfiles (
  lockfile: Lockfile | null,
  lockfileDir: string,
  prefix: string,
  opts: {
    autofixMergeConflicts?: boolean
    wantedVersions?: string[]
    ignoreIncompatible: boolean
  }
): Promise<Lockfile | null> {
  if (!lockfile) {
    return lockfile
  }
  const gitBranchLockfiles: Array<(Lockfile | null)> = (await _readGitBranchLockfiles(lockfileDir, prefix, opts)).map(({ lockfile }) => lockfile)

  let mergedLockfile: Lockfile = lockfile

  for (const gitBranchLockfile of gitBranchLockfiles) {
    if (!gitBranchLockfile) {
      continue
    }
    mergedLockfile = mergeLockfileChanges(mergedLockfile, gitBranchLockfile)
  }

  return mergedLockfile
}

async function _readGitBranchLockfiles (
  lockfileDir: string,
  prefix: string,
  opts: {
    autofixMergeConflicts?: boolean
    wantedVersions?: string[]
    ignoreIncompatible: boolean
  }
): Promise<Array<{
    lockfile: Lockfile | null
    hadConflicts: boolean
  }>> {
  const files = await getGitBranchLockfileNames(lockfileDir)

  return Promise.all(files.map((file) => _read(path.join(lockfileDir, file), prefix, opts)))
}
