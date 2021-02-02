import * as AMQP from 'amqplib'

import { AmqpConnection } from './amqpConnection'

function wrapConsumeHandler (handler: AmqpConsumer): ((msg: AMQP.ConsumeMessage | null) => void) {
  return async msg => await Promise.resolve(handler(msg))
    .catch(error => console.error(`Failed to handle '${msg?.fields.routingKey ?? 'null'}' message: ${error}`))
}

export type AmqpConsumer = (msg: AMQP.ConsumeMessage | null) => Promise<void> | void

interface StoredConsumer {
  queue: string
  consumer: AmqpConsumer
  options?: AMQP.Options.Consume
}

/**
 * Common super class for an amqp channel
 */
export class BaseAmqpChannel<T extends AMQP.Channel> {
  protected rawChannel?: T = undefined
  protected readonly consumers = new Map<string, StoredConsumer>()

  constructor (protected readonly connection: AmqpConnection) {
  }

  protected async createChannel (_connection: AMQP.Connection): Promise<T> {
    throw new Error('You have to implement the method createChannel()!')
  }

  async getRawChannel (): Promise<T> {
    if (this.rawChannel !== undefined) {
      return this.rawChannel
    }
    const rawConnection = await this.connection.getRawConnection()
    const channel = await this.createChannel(rawConnection)
    // Any error must be handled, otherwise the underlying connection will be closed.
    // The error ist just logged, because the `close` event will also be emitted after `error`
    channel.on('error', (error: any) => {
      console.log('Channel error:', error)
    })
    // If the raw channel has been closed it can not be reused, so we can remove the reference
    channel.on('close', () => {
      this.rawChannel = undefined
    })
    for (const storedConsumer of this.consumers.values()) {
      await channel.consume(storedConsumer.queue, wrapConsumeHandler(storedConsumer.consumer), storedConsumer.options)
    }
    this.rawChannel = channel
    return channel
  }

  /**
   * Closes this channel and removes all registered consumers.
   * Do not reuse the channel after calling the `close` method
   */
  async close (): Promise<void> {
    if (this.rawChannel !== undefined) {
      await this.rawChannel.close()
      this.consumers.clear()
      this.connection.removeChannel(this)
    }
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

  async consume (queue: string, onMessage: AmqpConsumer, options: AMQP.Options.Consume = {}):
  Promise<AMQP.Replies.Consume> {
    const channel = await this.getRawChannel()
    const response = await channel.consume(queue, wrapConsumeHandler(onMessage), options)
    options.consumerTag = response.consumerTag
    this.consumers.set(response.consumerTag, { queue, consumer: onMessage, options })
    return response
  }

  async cancel (consumerTag: string): Promise<AMQP.Replies.Empty> {
    const channel = await this.getRawChannel()
    const response = await channel.cancel(consumerTag)
    this.consumers.delete(consumerTag)
    return response
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

export class AmqpChannel extends BaseAmqpChannel<AMQP.Channel> {
  protected async createChannel (connection: AMQP.Connection): Promise<AMQP.Channel> {
    return await connection.createChannel()
  }
}

export class AmqpConfirmChannel extends BaseAmqpChannel<AMQP.ConfirmChannel> {
  protected async createChannel (connection: AMQP.Connection): Promise<AMQP.ConfirmChannel> {
    return await connection.createConfirmChannel()
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
