#!/usr/bin/env node

const H = require('highland')
const R = require('ramda')
const graphlib = require('graphlib')
const etl = require('spacetime-etl')

const argv = require('minimist')(process.argv.slice(2))

const graph = new graphlib.Graph({
  directed: true,
  compound: true,
  multigraph: true
})

const modes = [
  'list',
  'graph',
  'run'
]

if (!argv._ || argv._.length !== 1 || !modes.includes(argv._[0])) {
  console.log('usage: spacetime-orchestrator <command>')
  console.log()
  console.log('Possible commands:')
  console.log('  list      Outputs a JSON array with all ETL steps, in the right order')
  console.log('  graph     Outputs a JSON graph of ETL steps and their dependencies')
  console.log('  run       Runs all ETL steps, in the right order')
  process.exit()
}

const mode = argv._[0]

const modules = etl.modules()

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

const getList = () => graphlib.alg.topsort(graph).reverse()

if (mode === 'list') {
  console.log(JSON.stringify(getList(), null, 2))
} else if (mode === 'graph') {
  const graphJSON = graphlib.json.write(graph)

  console.log(JSON.stringify({
    nodes: graphJSON.nodes.map((node) => ({
      id: node.v
    })),
    edges: graphJSON.edges.map((edge) => ({
      source: edge.w,
      target: edge.v
    }))
  }, null, 2))
} else if (mode === 'run') {
  // Curried ETL function, with logging enabled
  const curriedExecute = R.curry(etl.execute)(R.__, R.__, true)

  H(getList())
    .map(curriedExecute)
    .nfcall([])
    .series()
    .stopOnError((err) => {
      console.error(err)
    })
    .done(() => {
      console.log('Done...')
    })
}
