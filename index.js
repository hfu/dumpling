const config = require('config')
const Queue = require('better-queue')
const fs = require('fs')
const zlib = require('zlib')
const { Pool, Query } = require('pg')
const wkx = require('wkx')
const tilebelt = require('@mapbox/tilebelt')
const bbox = require('@turf/bbox').default

const $pools = {}
const $z = config.get('z')
const $dst = config.get('dst')
const $files = {}
const $startTime = new Date()
let $count = 0
let $nTasks = 0

const watch = (relation) => {
  $count++
  if ($count % 10000 === 0) {
    const now = new Date()
    console.log(`${now.toISOString()}: ${$count} f, ${$nTasks} t, ${Math.round($count / (now - $startTime) * 1000)}f/s, ${relation}.`)
  }
}

const write = (z, x, y, s) => {
  const key = `${z}-${x}-${y}`
  if(!$files[key]) {
    $files[key] = zlib.createGzip()
    $files[key].pipe(fs.createWriteStream(`${$dst}/${key}.ndjson.gz`))
  }
  $files[key].write(`${s}\n`)
}

const fetch = (client, t) => {
  return new Promise(resolve => {
    let count = 0
    client.query(new Query(`FETCH 10000 FROM cur`))
    .on('row', row => {
      delete row.geom
      let f = {
        type: 'Feature',
	properties: row,
	geometry: null
      }
      f.geometry = JSON.parse(f.properties.st_asgeojson)
      delete f.properties.st_asgeojson
      f.properties._database = t.database
      f.properties._relation = t.relation
      const b = bbox(f)
      let [minx, maxy] = tilebelt.pointToTile(b[0], b[1], $z)
      if (maxy < 0) maxy = 0
      if (maxy > 2 ** $z - 1) maxy = 2 ** $z - 1
      if (minx < 0) minx = 0
      if (minx > 2 ** $z - 1) minx = 2 ** $z - 1
      let [maxx, miny] = tilebelt.pointToTile(b[2], b[3], $z)
      if (miny < 0) miny = 0
      if (miny > 2 ** $z - 1) miny = 2 ** $z - 1
      if (maxx < 0) maxx = 0
      if (maxx > 2 ** $z - 1) maxx = 2 ** $z - 1
      f = JSON.stringify(f)
      count++
      for (let x = minx; x <= maxx; x++) {
        for (let y = miny; y <= maxy; y++) {
	  queue.pause()
	  write($z, x, y, f)
	  queue.resume()
	}
      }
      watch(t.relation)
    })
    .on('error', err => {
      console.log(err.stack)
      throw err
    })
    .on('end', () => {
      resolve(count)
    })
  })
}

const queue = new Queue((t, cb) => {
  // console.log(t)
  $pools[t.database].connect(async (err, client, release) => {
    if (err) throw err
    let cols = await client.query(`SELECT column_name FROM information_schema.columns WHERE table_name='${t.relation}' ORDER BY ordinal_position`)
    cols = cols.rows.map(r => r.column_name).filter(r => !(r === 'geom'))
    await client.query(`BEGIN`)
    await client.query(`DECLARE cur CURSOR FOR SELECT ${cols.toString()},ST_AsGeoJSON(geom) FROM ${t.relation}`)
    while (await fetch(client, t) !== 0) {}
    await client.query(`COMMIT`)
    return cb()
  })
}, { concurrent: config.get('concurrent') })

for (const database of Object.keys(config.get('relations')).reverse()) {
  $pools[database] = new Pool({
    host: config.get('host'),
    user: config.get('user'),
    password: config.get('password'),
    database: database
  })
  for (const relation of config.get('relations')[database]) {
    queue.push({database: database, relation: relation})
  }
}

queue.on('task_queued', () => {
  $nTasks++
})

queue.on('task_finish', () => {
  $nTasks--
  if ($nTasks === 0) {
    for (const database in $pools) {
      $pools[database].end()
      console.log(`closed ${database}`)
    }
    for (const key in $files) {
      $files[key].end(() => {
        console.log(`closed ${$dst}/${key}.ndjson.gz`)
      })
    }
  }
})
