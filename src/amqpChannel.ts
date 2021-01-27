import * as AMQP from 'amqplib'

import { AmqpConnection } from './amqpConnection'

function wrapConsumeHandler (handler: (msg: AMQP.ConsumeMessage | null) => Promise<void> | void):
((msg: AMQP.ConsumeMessage | null) => | void) {
  return async msg => await Promise.resolve(handler(msg))
    .catch(error => console.error(`Failed to handle '${msg?.fields.routingKey ?? 'null'}' message: ${error}`))
}

/**
 * Common super class for an amqp channel
 */
class BaseAmqpChannel {
  async getRawChannel (): Promise<AMQP.Channel> {
    throw new Error('You have to implement the method getRawChannel()!')
  }

  async close (): Promise<void> {
    throw new Error('You have to implement the method close()!')
  }

  async assertQueue (queue: string, options?: AMQP.Options.AssertQueue): Promise<AMQP.Replies.AssertQueue> {
    const channel = await this.getRawChannel()
    return await channel.assertQueue(queue, options)
  }

  async checkQueue (queue: string): Promise<AMQP.Replies.AssertQueue> {
    const channel = await this.getRawChannel()
    return await channel.checkQueue(queue)
  }

  async deleteQueue (queue: string, options?: AMQP.Options.DeleteQueue): Promise<AMQP.Replies.DeleteQueue> {
    const channel = await this.getRawChannel()
    return await channel.deleteQueue(queue, options)
  }

  async purgeQueue (queue: string): Promise<AMQP.Replies.PurgeQueue> {
    const channel = await this.getRawChannel()
    return await channel.purgeQueue(queue)
  }

  async bindQueue (queue: string, source: string, pattern: string, args?: any): Promise<AMQP.Replies.Empty> {
    const channel = await this.getRawChannel()
    return await channel.bindQueue(queue, source, pattern, args)
  }

  async unbindQueue (queue: string, source: string, pattern: string, args?: any): Promise<AMQP.Replies.Empty> {
    const channel = await this.getRawChannel()
    return await channel.unbindQueue(queue, source, pattern, args)
  }

  async assertExchange (exchange: string, type: string, options?: AMQP.Options.AssertExchange):
  Promise<AMQP.Replies.AssertExchange> {
    const channel = await this.getRawChannel()
    return await channel.assertExchange(exchange, type, options)
  }

  async checkExchange (exchange: string): Promise<AMQP.Replies.Empty> {
    const channel = await this.getRawChannel()
    return await channel.checkExchange(exchange)
  }

  async deleteExchange (exchange: string, options?: AMQP.Options.DeleteExchange): Promise<AMQP.Replies.Empty> {
    const channel = await this.getRawChannel()
    return await channel.deleteExchange(exchange, options)
  }

  async bindExchange (destination: string, source: string, pattern: string, args?: any): Promise<AMQP.Replies.Empty> {
    const channel = await this.getRawChannel()
    return await channel.bindExchange(destination, source, pattern, args)
  }

  async unbindExchange (destination: string, source: string, pattern: string, args?: any): Promise<AMQP.Replies.Empty> {
    const channel = await this.getRawChannel()
    return await channel.unbindExchange(destination, source, pattern, args)
  }

  async publish (exchange: string, routingKey: string, content: Buffer, options?: AMQP.Options.Publish):
  Promise<boolean> {
    const channel = await this.getRawChannel()
    return channel.publish(exchange, routingKey, content, options)
  }

  async sendToQueue (queue: string, content: Buffer, options?: AMQP.Options.Publish): Promise<boolean> {
    const channel = await this.getRawChannel()
    return channel.sendToQueue(queue, content, options)
  }

  async consume (queue: string, onMessage: (msg: AMQP.ConsumeMessage | null) => Promise<void> | void,
    options?: AMQP.Options.Consume): Promise<AMQP.Replies.Consume> {
    const channel = await this.getRawChannel()
    return await channel.consume(queue, wrapConsumeHandler(onMessage), options)
  }

  async cancel (consumerTag: string): Promise<AMQP.Replies.Empty> {
    const channel = await this.getRawChannel()
    return await channel.cancel(consumerTag)
  }

  async get (queue: string, options?: AMQP.Options.Get): Promise<AMQP.GetMessage | false> {
    const channel = await this.getRawChannel()
    return await channel.get(queue, options)
  }

  async ack (message: AMQP.Message, allUpTo?: boolean): Promise<void> {
    const channel = await this.getRawChannel()
    return channel.ack(message, allUpTo)
  }

  async ackAll (): Promise<void> {
    const channel = await this.getRawChannel()
    return channel.ackAll()
  }

  async nack (message: AMQP.Message, allUpTo?: boolean, requeue?: boolean): Promise<void> {
    const channel = await this.getRawChannel()
    return channel.nack(message, allUpTo, requeue)
  }

  async nackAll (requeue?: boolean): Promise<void> {
    const channel = await this.getRawChannel()
    return channel.nackAll(requeue)
  }

  async reject (message: AMQP.Message, requeue?: boolean): Promise<void> {
    const channel = await this.getRawChannel()
    return channel.reject(message, requeue)
  }

  async prefetch (count: number, global?: boolean): Promise<AMQP.Replies.Empty> {
    const channel = await this.getRawChannel()
    return await channel.prefetch(count, global)
  }

  async recover (): Promise<AMQP.Replies.Empty> {
    const channel = await this.getRawChannel()
    return await channel.recover()
  }
}

export class AmqpChannel extends BaseAmqpChannel {
  private rawChannel?: AMQP.Channel = undefined

  constructor (protected readonly connection: AmqpConnection) {
    super()
  }

  async getRawChannel (): Promise<AMQP.Channel> {
    if (this.rawChannel !== undefined) {
      return this.rawChannel
    }
    const rawConnection = await this.connection.getRawConnection()
    const channel = await rawConnection.createChannel()
    // Any error must be handled, otherwise the underlying connection will be closed.
    // The error ist just logged, because the `close` event will also be emitted after `error`
    channel.on('error', (error: any) => {
      console.log('Channel error:', error)
    })
    // If the channel has been closed it can not be reused, so we can remove the reference
    channel.on('close', () => {
      this.rawChannel = undefined
    })
    this.rawChannel = channel
    return channel
  }

  async close (): Promise<void> {
    if (this.rawChannel !== undefined) {
      await this.rawChannel.close()
    }
  }
}

export class AmqpConfirmChannel extends BaseAmqpChannel {
  private rawChannel?: AMQP.ConfirmChannel = undefined

  constructor (protected readonly connection: AmqpConnection) {
    super()
  }

  async getRawChannel (): Promise<AMQP.ConfirmChannel> {
    if (this.rawChannel !== undefined) {
      return this.rawChannel
    }
    const rawConnection = await this.connection.getRawConnection()
    const channel = await rawConnection.createConfirmChannel()
    // Any error must be handled, otherwise the underlying connection will be closed.
    // The error ist just logged, because the `close` event will also be emitted after `error`
    channel.on('error', (error: any) => {
      console.log('Channel error:', error)
    })
    // If the channel has been closed it can not be reused, so we can remove the reference
    channel.on('close', () => {
      this.rawChannel = undefined
    })
    this.rawChannel = channel
    return channel
  }

  async close (): Promise<void> {
    if (this.rawChannel !== undefined) {
      await this.rawChannel.close()
    }
  }

  async publish (exchange: string, routingKey: string, content: Buffer,
    options?: AMQP.Options.Publish, callback?: (err: any, ok: AMQP.Replies.Empty) => void): Promise<boolean> {
    const channel = await this.getRawChannel()
    return channel.publish(exchange, routingKey, content, options, callback)
  }

  async sendToQueue (queue: string, content: Buffer,
    options?: AMQP.Options.Publish, callback?: (err: any, ok: AMQP.Replies.Empty) => void): Promise<boolean> {
    const channel = await this.getRawChannel()
    return channel.sendToQueue(queue, content, options, callback)
  }

  async waitForConfirms (): Promise<void> {
    const channel = await this.getRawChannel()
    return await channel.waitForConfirms()
  }
}
