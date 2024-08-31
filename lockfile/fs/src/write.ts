import { promises as fs } from 'fs'
import path from 'path'
import assert from 'node:assert'
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
import { DEPENDENCIES_FIELDS } from '@pnpm/types'

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
  }
): Promise<void> {
  console.log(`writeWantedLockfile! pkgPath=${pkgPath}`)
  logger.info({ message: 'attempt to write...', prefix: pkgPath })
  const wantedLockfileName: string = await getWantedLockfileName(opts)
  return writeLockfile(wantedLockfileName, pkgPath, wantedLockfile, { lockfilePerWorkspacePackage: true })
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
    lockfilePerWorkspacePackage: boolean
  }
): Promise<void> {
  const lockfilePathForRoot = path.join(pkgPath, lockfileFilename)

  const completeLockfile = convertToLockfileFile(wantedLockfile, {
    forceSharedFormat: true,
  })

  let wantedLockfileForRootToStringify
  if (!opts.lockfilePerWorkspacePackage) {
    wantedLockfileForRootToStringify = completeLockfile
  } else {
    const wantedLockfileForRoot = filterLockfileByImporters(wantedLockfile, ['.' as ProjectId], { include: { dependencies: true, devDependencies: true, optionalDependencies: true }, skipped: new Set(), failOnMissingDependencies: true })
    wantedLockfileForRootToStringify = convertToLockfileFile(wantedLockfileForRoot, {
      forceSharedFormat: true,
    })
    assert(wantedLockfileForRootToStringify.importers)

    if (completeLockfile.importers) {
      const projectIdsExcludingRoot = Object.keys(completeLockfile.importers).filter(projectId => projectId !== '.') as ProjectId[]
      await Promise.all(projectIdsExcludingRoot.map(async projectId => {
        const lockfilePathForProject = path.join(pkgPath, projectId, lockfileFilename)

        const wantedLockfileForProject = filterLockfileByImporters(wantedLockfile, [projectId], { include: { dependencies: true, devDependencies: true, optionalDependencies: true }, skipped: new Set(), failOnMissingDependencies: true })
        const lockfileForProjectToStringify = convertToLockfileFile(wantedLockfileForProject, {
          forceSharedFormat: true,
        })

        const yamlDoc = yamlStringify(lockfileForProjectToStringify)

        await writeFileAtomic(lockfilePathForProject, yamlDoc)
      }))

      for (const projectId of (Object.keys(completeLockfile.importers) as ProjectId[])) {
        wantedLockfileForRootToStringify.importers[projectId] = { }
        const inlineSpecifiersProjectSnapshot = completeLockfile.importers[projectId]
        for (const depField of DEPENDENCIES_FIELDS) {
          const inlineSpecifiersResolvedDependencies = inlineSpecifiersProjectSnapshot[depField]
          if (inlineSpecifiersResolvedDependencies) {
            wantedLockfileForRootToStringify.importers[projectId][depField] = Object.fromEntries(
              Object.entries(inlineSpecifiersResolvedDependencies)
                .filter(([_depName, specifierAndResolution]) => specifierAndResolution.specifier.startsWith('workspace:')))
          }
        }
      }
    }
  }

  const yamlDoc = yamlStringify(wantedLockfileForRootToStringify)

  return writeFileAtomic(lockfilePathForRoot, yamlDoc)
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
  }
): Promise<void> {
  const wantedLockfileName: string = await getWantedLockfileName(opts)
  const wantedLockfilePath = path.join(opts.wantedLockfileDir, wantedLockfileName)
  const currentLockfilePath = path.join(opts.currentLockfileDir, 'lock.yaml')

  const normalizeOpts = {
    forceSharedFormat: true,
  }
  const wantedLockfileToStringify = convertToLockfileFile(opts.wantedLockfile, normalizeOpts)
  const yamlDoc = yamlStringify(wantedLockfileToStringify)

  // in most cases the `pnpm-lock.yaml` and `node_modules/.pnpm-lock.yaml` are equal
  // in those cases the YAML document can be stringified only once for both files
  // which is more efficient
  if (opts.wantedLockfile === opts.currentLockfile) {
    const lockfileFilesToWrite = computeLockfileFilesToWrite({ wantedLockfileDir: opts.wantedLockfileDir, completeLockfile: opts.wantedLockfile })
    await Promise.all([
      ...lockfileFilesToWrite.map(async lockfileFile => writeFileAtomic(path.join(lockfileFile.lockfileDir, wantedLockfileName), yamlStringify(lockfileFile.lockfileFile))),
      (async () => {
        if (isEmptyLockfile(opts.wantedLockfile)) {
          await rimraf(currentLockfilePath)
        } else {
          await fs.mkdir(path.dirname(currentLockfilePath), { recursive: true })
          await writeFileAtomic(currentLockfilePath, yamlDoc)
        }
      })(),
    ])
    return
  }

  logger.debug({
    message: `\`${WANTED_LOCKFILE}\` differs from \`${path.relative(opts.wantedLockfileDir, currentLockfilePath)}\``,
    prefix: opts.wantedLockfileDir,
  })

  const currentLockfileToStringify = convertToLockfileFile(opts.currentLockfile, normalizeOpts)
  const currentYamlDoc = yamlStringify(currentLockfileToStringify)

  console.log(`wantedLockfilePath=${wantedLockfilePath}, currentLockfilePath=${currentLockfilePath}`)

  const lockfileFilesToWrite = computeLockfileFilesToWrite({ wantedLockfileDir: opts.wantedLockfileDir, completeLockfile: opts.wantedLockfile })

  await Promise.all([
    ...lockfileFilesToWrite.map(async lockfileFile => writeFileAtomic(path.join(lockfileFile.lockfileDir, wantedLockfileName), yamlStringify(lockfileFile.lockfileFile))),
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

function computeLockfileFilesToWrite (
  opts: {
    completeLockfile: Lockfile
    wantedLockfileDir: string
  }
): Array<{ lockfileDir: string, lockfileFile: LockfileFile }> {
  const wantedLockfileForRoot = filterLockfileByImporters(opts.completeLockfile, ['.' as ProjectId], { include: { dependencies: true, devDependencies: true, optionalDependencies: true }, skipped: new Set(), failOnMissingDependencies: true })
  const wantedLockfileForRootToStringify = convertToLockfileFile(wantedLockfileForRoot, { forceSharedFormat: true })
  assert(wantedLockfileForRootToStringify.importers)

  let projectsLockfiles: Array<{ lockfileDir: string, lockfileFile: LockfileFile }> = []
  if (opts.completeLockfile.importers) {
    const projectIdsExcludingRoot = Object.keys(opts.completeLockfile.importers).filter(projectId => projectId !== '.') as ProjectId[]
    projectsLockfiles = projectIdsExcludingRoot.map(projectId => {
      let wantedLockfileForProject = filterLockfileByImporters(opts.completeLockfile, [projectId], { include: { dependencies: true, devDependencies: true, optionalDependencies: true }, skipped: new Set(), failOnMissingDependencies: true })
      wantedLockfileForProject = {
        ...wantedLockfileForProject,
        importers: Object.fromEntries(Object.entries(wantedLockfileForProject.importers).filter(([projectIdOfEntry]) => projectIdOfEntry === projectId)),
      }
      const lockfileForProjectToStringify = convertToLockfileFile(wantedLockfileForProject, {
        forceSharedFormat: true,
      })

      return {
        lockfileDir: path.join(opts.wantedLockfileDir, projectId),
        lockfileFile: lockfileForProjectToStringify,
      }
    })

    const completeLockfileFile = convertToLockfileFile(opts.completeLockfile, { forceSharedFormat: true })
    assert(completeLockfileFile.importers)
    for (const projectId of (Object.keys(completeLockfileFile.importers) as ProjectId[])) {
      wantedLockfileForRootToStringify.importers[projectId] = { }
      const inlineSpecifiersProjectSnapshot = completeLockfileFile.importers[projectId]
      for (const depField of DEPENDENCIES_FIELDS) {
        const inlineSpecifiersResolvedDependencies = inlineSpecifiersProjectSnapshot[depField]
        if (inlineSpecifiersResolvedDependencies) {
          wantedLockfileForRootToStringify.importers[projectId][depField] = Object.fromEntries(
            Object.entries(inlineSpecifiersResolvedDependencies)
              .filter(([_depName, specifierAndResolution]) => specifierAndResolution.specifier.startsWith('workspace:')))
        }
      }
    }
  }

  return [{ lockfileDir: opts.wantedLockfileDir, lockfileFile: wantedLockfileForRootToStringify }, ...projectsLockfiles]
}