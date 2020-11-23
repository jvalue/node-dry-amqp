import * as AMQP from 'amqplib'
import { sleep, stringifiers } from '@jvalue/node-dry-basics'

import * as AmqpConnector from './amqpConnector'

const LOG_MAX_LENGTH = 30

export class AmqpPublisher {
  private channel?: AMQP.Channel

  public async init (
    amqpUrl: string,
    retries: number,
    msBackoff: number,
    exchange: { name: string, type: string },
    exchangeOptions: AMQP.Options.AssertExchange
  ): Promise<void> {
    for (let i = 1; i <= retries; i++) {
      try {
        const connection = await AmqpConnector.connect(amqpUrl)
        this.channel = await AmqpConnector.initChannel(connection, exchange, exchangeOptions)
        return
      } catch (error) {
        console.info(`Error initializing the AMQP Client (${i}/${retries}):
        ${error}. Retrying in ${msBackoff}...`)
        await sleep(msBackoff)
      }
    }
    throw new Error(`Could not connect to AMQP broker at ${amqpUrl}`)
  }

  public publish (exchangeName: string, routingKey: string, content: object): boolean {
    if (this.channel === undefined) {
      console.error('Publish not possible, AMQP client not initialized.')
      return false
    } else {
      try {
        const success = this.channel.publish(
          exchangeName,
          routingKey,
          Buffer.from(JSON.stringify(content))
        )
        console.debug(`[AMQP][Produce] ${routingKey}: ${stringifiers.stringify(content, LOG_MAX_LENGTH)}`)
        return success
      } catch (error) {
        console.error(`Error publishing to exchange ${exchangeName} under key ${routingKey}: ${error}`)
        return false
      }
    }
  }
}
