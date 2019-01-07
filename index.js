const config = require('config')
const Queue = require('better-queue')
const fs = require('fs')

const $o = fs.createWriteStream('a.ndjson')
const $pools = {}

const queue = new Queue((t, cb) => {
  console.log(t)
  cb()
}, { concurrent: config.get('concurrent') })

for (const database of Object.keys(config.get('relations'))) {
  console.log(database)
}

process.on('exit', code => {
  $o.close()
})
