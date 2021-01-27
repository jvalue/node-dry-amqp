import * as AMQP from 'amqplib'

import * as AmqpConnector from './amqpConnector'
import { AmqpChannel, AmqpConfirmChannel } from './amqpChannel'

export class AmqpConnection {
  private rawConnection?: AMQP.Connection = undefined
  constructor (
    private readonly amqpUrl: string,
    private readonly retries?: number,
    private readonly retryDelayInMs?: number
  ) {}

  async close (): Promise<void> {
    if (this.rawConnection !== undefined) {
      await this.rawConnection.close()
      this.rawConnection = undefined
    }
  }

  async getRawConnection (): Promise<AMQP.Connection> {
    if (this.rawConnection !== undefined) {
      return this.rawConnection
    }
    const connection = await AmqpConnector.connect(this.amqpUrl, this.retries, this.retryDelayInMs)
    // Handle any error by just logging it, because the `close` event will also be emitted after `error`
    connection.on('error', (error: any) => {
      console.info('AMQP connection closed unexpectedly:', error)
    })
    // If the connection has been closed it can not be reused, so we can remove the reference
    connection.on('close', () => {
      this.rawConnection = undefined
    })
    this.rawConnection = connection
    return connection
  }

  async createChannel (): Promise<AmqpChannel> {
    return new AmqpChannel(this)
  }

  async createConfirmChannel (): Promise<AmqpConfirmChannel> {
    return new AmqpConfirmChannel(this)
  }
}
