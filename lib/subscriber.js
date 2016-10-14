import amqp from 'amqplib'
import Debug from 'debug'
import retry from 'retry'

const debug = Debug('amqp')

class Subscriber {
  constructor (connectionString, options = {}) {
    if (!connectionString) {
      throw new Error('Connection string is required')
    }
    if (!options.exchange) {
      throw new Error('Exchange required')
    }
    if (!options.queue) {
      throw new Error('Queue required')
    }

    this._channel = null
    this._queue = null
    this._options = Object.assign({
      type: 'fanout',
      connection: connectionString,
      durable: true,
      routingKeys: [''],
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

  async start (callback) {
    const options = this._options

    debug('starting subscriber')
    const connection = await this._connect()
    debug('connection success')
    this._channel = await connection.createChannel()
    debug('connected to channel')
    debug(`asserting exchange ${options.exchange}`)
    await this._channel.assertExchange(options.exchange, options.type, options.durable)
    debug(`asserting queue ${options.queue}`)
    await this._channel.assertQueue(options.queue)
    this._queue = options.queue

    debug('binding queue:')
    for (let routeKey of options.routingKeys) {
      debug(`\t*using route key ${routeKey}`)
      await this._channel.bindQueue(this._queue, options.exchange, routeKey)
    }

    this._channel.prefetch(1)
    debug('consuming from queue')
    return this._channel.consume(this._queue, callback.bind(this), {
      noAck: false
    })
  }

  ack (message) {
    debug('sending ack')
    this._channel.ack(message)
  }
}

export default Subscriber
