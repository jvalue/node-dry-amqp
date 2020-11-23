import * as AMQP from 'amqplib'
import { sleep, stringifiers } from '@jvalue/node-dry-basics'

import * as AmqpConnector from './amqpConnector'

const LOG_MAX_LENGTH = 30

export class AmqpConsumer {
  private connection?: AMQP.Connection

  public async init (amqpUrl: string, retries: number, msBackoff: number): Promise<void> {
    for (let i = 1; i <= retries; i++) {
      try {
        this.connection = await AmqpConnector.connect(amqpUrl)
        return
      } catch (error) {
        console.info(`Error initializing the AMQP Client (${i}/${retries}):
        ${error}. Retrying in ${msBackoff}...`)
      }
      await sleep(msBackoff)
    }
    throw new Error(`Could not connect to AMQP broker at ${amqpUrl}`)
  }

  public async registerConsumer (
    exchange: { name: string, type: string },
    exchangeOptions: AMQP.Options.AssertExchange,
    queue: { name: string, routingKey: string },
    queueOptions: AMQP.Options.AssertQueue,
    consumeEvent: (msg: AMQP.ConsumeMessage | null) => Promise<void>
  ): Promise<void> {
    if (this.connection === undefined) {
      throw new Error('Consume not possible, AMQP client not initialized.')
    }

    try {
      const channel =
        await AmqpConnector.initChannel(this.connection, { name: exchange.name, type: exchange.type }, exchangeOptions)
      const q = await channel.assertQueue(queue.name, queueOptions)
      await channel.bindQueue(q.queue, exchange.name, queue.routingKey)
      await channel.consume(q.queue, msg => {
        console.debug("[AMQP][Consume] %s:'%s'",
          msg?.fields.routingKey, stringifiers.stringify(msg?.content.toString(), LOG_MAX_LENGTH))
        consumeEvent(msg)
          .catch(error => console.error(`Failed to handle ${msg?.fields.routingKey ?? 'null'} event`, error))
      })
    } catch (error) {
      throw new Error(`Error subscribing to exchange ${exchange.name} under key ${queue.routingKey}: ${error}`)
    }
  }
}
