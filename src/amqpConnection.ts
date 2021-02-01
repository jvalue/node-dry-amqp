import * as AMQP from 'amqplib'

import * as AmqpConnector from './amqpConnector'
import { AmqpChannel, AmqpConfirmChannel, BaseAmqpChannel } from './amqpChannel'

export class AmqpConnection {
  private rawConnection?: AMQP.Connection = undefined
  private currentCreateConnectionPromise?: Promise<AMQP.Connection> = undefined
  private readonly channels: Set<BaseAmqpChannel<AMQP.Channel>> = new Set()

  constructor (
    private readonly amqpUrl: string,
    private readonly retries?: number,
    private readonly retryDelayInMs?: number,
    private readonly connectionLossHandler: (error: any) => void = () => {}
  ) {}

  async close (): Promise<void> {
    if (this.rawConnection !== undefined) {
      for (const channel of this.channels) {
        await channel.close()
      }
      this.channels.clear()
      await this.rawConnection.close()
      this.rawConnection = undefined
    }
  }

  async getRawConnection (): Promise<AMQP.Connection> {
    if (this.rawConnection !== undefined) {
      return this.rawConnection
    }
    if (this.currentCreateConnectionPromise === undefined) {
      this.currentCreateConnectionPromise = this.createRawConnection()
    }
    try {
      this.rawConnection = await this.currentCreateConnectionPromise
    } finally {
      this.currentCreateConnectionPromise = undefined
    }
    return this.rawConnection
  }

  private async createRawConnection (): Promise<AMQP.Connection> {
    const connection = await AmqpConnector.connect(this.amqpUrl, this.retries, this.retryDelayInMs)
    // Handle any error by just logging it, because the `close` event will also be emitted after `error`
    connection.on('error', (error: any) => {
      console.info('AMQP connection errorred unexpectedly:', error)
      // Try to reconnect
      setTimeout(() => {
        this.getRawConnection().catch(this.connectionLossHandler)
      }, this.retryDelayInMs)
    })
    // If the connection has been closed it can not be reused, so we can remove the reference
    connection.on('close', () => {
      this.rawConnection = undefined
    })
    this.rawConnection = connection
    await this.reconnectChannels()
    return connection
  }

  private async reconnectChannels (): Promise<void> {
    for (const channel of this.channels) {
      await channel.getRawChannel()
    }
  }

  removeChannel (channel: BaseAmqpChannel<AMQP.Channel>): void {
    this.channels.delete(channel)
  }

  async createChannel (): Promise<AmqpChannel> {
    const channel = new AmqpChannel(this)
    this.channels.add(channel)
    return channel
  }

  async createConfirmChannel (): Promise<AmqpConfirmChannel> {
    const channel = new AmqpConfirmChannel(this)
    this.channels.add(channel)
    return channel
  }
}
