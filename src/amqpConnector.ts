import * as AMQP from 'amqplib'

export async function connect (amqpUrl: string): Promise<AMQP.Connection> {
  try {
    const connection = await AMQP.connect(amqpUrl)
    console.info(`Connection to amqp host at ${amqpUrl} successful`)
    return connection
  } catch (error) {
    console.error(`Error connecting to amqp host at ${amqpUrl}: ${error}`)
    throw error
  }
}

export async function initChannel (
  connection: AMQP.Connection,
  exchange: {name: string, type: string},
  exchangeOptions: AMQP.Options.AssertExchange
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
