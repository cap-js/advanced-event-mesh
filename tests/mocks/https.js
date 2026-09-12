// https fake for the token endpoint. Wired via
// `jest.mock('https', () => require('../mocks/https'))`.

const { Readable } = require('stream')
const noop = () => {}

module.exports = {
  Agent: class {},
  request: (url, opts, cb) => {
    const res = new Readable()
    res.push(JSON.stringify({ access_token: '<sampleToken>', expires_in: 1 }))
    res.push(null)
    Object.assign(res, { headers: { 'content-type': 'application/json' } })
    setTimeout(() => cb(res), 1)
    return { on: noop, write: noop, end: noop }
  }
}
