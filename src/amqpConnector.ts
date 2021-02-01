import * as AMQP from 'amqplib'
import { sleep } from '@jvalue/node-dry-basics'

export async function connect (amqpUrl: string, retries = 0, retryDelayInMs = 2000): Promise<AMQP.Connection> {
  for (let i = 0; i <= retries; i++) {
    try {
      return await AMQP.connect(amqpUrl)
    } catch (error) {
      if (i === retries) {
        throw error
      }
      console.info(`Failed to connect to AMQP: ${error}. ` +
        `Performing retry ${i + 1}/${retries} in ${retryDelayInMs}ms`)
      await sleep(retryDelayInMs)
    }
  }
  throw new Error(`Could not connect to AMQP broker at ${amqpUrl}`)
}

export async function initChannel (
  connection: AMQP.Connection,
  exchange: {name: string, type: string},
  exchangeOptions?: AMQP.Options.AssertExchange
): Promise<AMQP.Channel> {
  try {
    const channel = await connection.createChannel()
    await channel.assertExchange(exchange.name, exchange.type, exchangeOptions)
    console.info(`Exchange ${exchange.name} successfully initialized.`)
    return channel
  } catch (error) {
    console.error(`Error creating exchange ${exchange.name}: ${error}`)
    throw error
  }
}
