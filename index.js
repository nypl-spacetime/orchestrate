'use strict'

const fs = require('fs')
const path = require('path')
const R = require('ramda')
const graphlib = require('graphlib')

const graph = new graphlib.Graph({
  directed: true,
  compound: true,
  multigraph: true
})

// TODO: use spacetime-config
//   - modulePrefix
//   - moduleDir
const baseDir = '/Users/bertspaan/code/etl-modules'

function isDirectory (module) {
  if (module.dir === '.') {
    return false
  }

  var stat = fs.statSync(path.join(module.baseDir, module.dir))
  return stat.isDirectory()
}

function getFilename (type, module) {
  return path.join(module.baseDir, module.dir, `${module.id}.${type}`)
}

function containsFile (type, module) {
  return fs.existsSync(getFilename(type, module))
}

function allModules (baseDir, type) {
  if (!type) {
    return []
  }

  return fs.readdirSync(baseDir)
    .map((file) => ({
      id: file.replace('etl-', ''),
      dir: file,
      baseDir: baseDir
    }))
    .filter(isDirectory)
    .filter(R.curry(containsFile)('js'))
    .filter(R.curry(containsFile)('dataset.json'))
    .map((module) => Object.assign(module, {
      dataset: require(getFilename('dataset.json', module)),
      steps: require(getFilename('js', module)).steps.map((fn) => fn.name)
    }))
}

const modules = allModules(baseDir, '.js')

// Add all modules, and their steps
modules.forEach((module) => {
  if (module.steps && module.steps.length) {
    module.steps
      .forEach((step) => graph.setNode(`${module.id}.${step}`))

    for (let i = module.steps.length - 1; i > 0; i--) {
      const from = `${module.id}.${module.steps[i]}`
      const to = `${module.id}.${module.steps[i - 1]}`
      graph.setEdge(from, to)
    }
  }
})

modules.forEach((module) => {
  if (module.dataset.dependsOn && module.dataset.dependsOn.length) {
    const from = `${module.id}.${module.steps[0]}`
    module.dataset.dependsOn
      .forEach((dependsOn) => {
        if (dependsOn.startsWith('*.')) {
          const dependsOnStep = dependsOn.replace('*.', '')
          const moduleIds = modules
            .filter((module) => module.steps.includes(dependsOnStep))
            .map((module) => module.id)

          moduleIds
            .map((moduleId) => `${moduleId}.${dependsOnStep}`)
            .forEach((to) => graph.setEdge(from, to))
        } else {
          // TODO: see if node dependsOn exists

          graph.setEdge(from, dependsOn)
        }
      })
  }
})

console.log(graphlib.alg.topsort(graph).reverse())
