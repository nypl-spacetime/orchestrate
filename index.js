#!/usr/bin/env node

const H = require('highland')
const R = require('ramda')
const graphlib = require('graphlib')
const etl = require('spacetime-etl')
const argv = require('minimist')(process.argv.slice(2), {
  boolean: 'steps',
  default: {
    steps: true
  }
})

const graph = new graphlib.Graph({
  directed: true,
  compound: true,
  multigraph: true
})

const modes = [
  'list',
  'graph',
  'dot',
  'run'
]

if (!argv._ || argv._.length !== 1 || !modes.includes(argv._[0])) {
  const help = [
      'usage: spacetime-orchestrator <options> <command>',
      '',
      'Options:',
      '  --steps, --no-steps    Output individual steps, or treat modules as one (default: steps)',
      '',
      'Possible commands:',
      '  list                   Outputs a JSON array with all ETL steps, in the right order',
      '  graph                  Outputs a JSON graph of ETL steps and their dependencies',
      '  dot                    Outputs the same graph, in Graphviz DOT Language',
      '  run                    Runs all ETL steps, in the right order'
  ]
  console.log(help.join('\n'))
  process.exit()
}

const useSteps = argv.steps
const mode = argv._[0]

const modules = etl.modules()

// Add all modules, and their steps
modules.forEach((module) => {
  if (module.steps && module.steps.length) {
    if (useSteps) {
      module.steps
        .forEach((step) => graph.setNode(`${module.id}.${step}`))

      for (let i = module.steps.length - 1; i > 0; i--) {
        const from = `${module.id}.${module.steps[i]}`
        const to = `${module.id}.${module.steps[i - 1]}`
        graph.setEdge(from, to)
      }
    } else {
      graph.setNode(module.id)
    }
  }
})

modules.forEach((module) => {
  if (module.dataset.dependsOn && module.dataset.dependsOn.length) {
    let from
    if (useSteps) {
      from = `${module.id}.${module.steps[0]}`
    } else {
      from = module.id
    }

    module.dataset.dependsOn
      .forEach((dependsOn) => {
        if (dependsOn.startsWith('*.')) {
          const dependsOnStep = dependsOn.replace('*.', '')
          const moduleIds = modules
            .filter((module) => module.steps.includes(dependsOnStep))
            .map((module) => module.id)

          if (useSteps) {
            moduleIds
              .map((moduleId) => `${moduleId}.${dependsOnStep}`)
              .forEach((to) => graph.setEdge(from, to))
          } else {
            moduleIds
              .forEach((moduleId) => graph.setEdge(from, moduleId))
          }
        } else {
          // TODO: see if node dependsOn exists
          if (useSteps) {
            graph.setEdge(from, dependsOn)
          } else {
            graph.setEdge(from, dependsOn.split('.')[0])
          }
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
} else if (mode === 'dot') {
  const graphJSON = graphlib.json.write(graph)
  const dotLines = [
    'digraph spacetime {',
    ...graphJSON.edges.map((edge) => `  "${edge.w.split('.')[0]}" -> "${edge.v.split('.')[0]}";`),
    '}'
  ]

  console.log(dotLines.join('\n'))
} else if (mode === 'run') {
  // Curried ETL function, with logging enabled
  const curriedExecute = R.curry(etl.execute)(R.__, R.__, true)

  H(getList())
    .map(curriedExecute)
    .nfcall([])
    .series()
    .done(() => {
      console.log('Done...')
    })
}
