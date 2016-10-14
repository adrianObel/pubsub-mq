import amqp from 'amqplib'
import Debug from 'debug'
import retry from 'retry'

const debug = Debug('amqp')

class Publisher {
  constructor (connectionString, options = {}) {
    if (!connectionString) {
      throw new Error('Connection string is required')
    }
    if (!options.exchange) {
      throw new Error('Exchange required')
    }

    this._channel = null
    this._queue = null
    this._options = Object.assign({
      type: 'fanout',
      connection: connectionString,
      durable: true,
      maxRetries: 10,
      retryInterval: 2000
    }, options)
  }

  _connect () {
    debug('connecting to RabbitMQ')
    const options = this._options
    const operation = retry.operation({
      retries: options.maxRetries,
      minTimeout: options.retryInterval
    })

    return new Promise((resolve, reject) => {
      operation.attempt(async (currentAttepmt) => {
        try {
          const connection = await amqp.connect(options.connection)
          resolve(connection)
        } catch (error) {
          debug('failed to connect')
          operation.retry(error)

          if (currentAttepmt <= options.maxRetries) {
            debug(`retrying in ${options.retryInterval / 1000} seconds`)
          } else {
            reject(error)
          }
        }
      })
    })
  }

  async start () {
    const options = this._options

    debug('starting publisher')
    const connection = await this._connect()
    debug('connection success')
    this._channel = await connection.createChannel()
    debug('connected to channel')
    debug(`asserting exchange ${options.exchange}`)
    return this._channel.assertExchange(options.exchange, options.type, options.durable)
  }

  async publish (routeKey, message) {
    if (!this._channel) {
      throw new Error('Connection not found. Make sure you first call publisher.start')
    }

    debug('publishing message')
    debug(`\nexchange: ${this._options.exchange}`)
    debug(`\nroutingKey: ${routeKey}`)
    debug(`\ncontent: ${message}`)

    return this._channel.publish(this._options.exchange, routeKey, new Buffer(message))
  }

  ack (message) {
    debug('sending ack')
    this._channel.ack(message)
  }

  nack (message) {
    debug('sending nack')
    this._channel.nack(message)
  }
}

export default Publisher
