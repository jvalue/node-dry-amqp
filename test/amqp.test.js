/* eslint-env jest */
const { promisify } = require('util')
const exec = promisify(require('child_process').exec)

const { sleep } = require('@jvalue/node-dry-basics')

const AmqpConnector = require('../src/amqpConnector')
const { AmqpConnection } = require('../src/amqpConnection')

const AMQP_PORT = 5672
const RABBIT_MQ_UI_PORT = 15672
const CONTAINER_NAME = 'node-dry-amqp-test-broker'
const RABBIT_MQ_USER = 'rabbit_user'
const RABBIT_MQ_PASSWORD = 'rabbit-password'

const AMQP_URL = `amqp://${RABBIT_MQ_USER}:${RABBIT_MQ_PASSWORD}@localhost:${AMQP_PORT}`

const CONNECT_RETRIES = 5
const CONNECT_RETRY_DELAY_MS = 5000

const TEST_TIMEOUT = 120000
const PUBLICATION_DELAY_MS = 1000

async function startRabbitMQ () {
  await exec(`docker run -p ${AMQP_PORT}:5672 -p ${RABBIT_MQ_UI_PORT}:15672 --name ${CONTAINER_NAME} -d ` +
    `-e RABBITMQ_DEFAULT_USER=${RABBIT_MQ_USER} -e RABBITMQ_DEFAULT_PASS=${RABBIT_MQ_PASSWORD} ` +
    'rabbitmq:3-management-alpine')
}

describe('node-dry-amqp test', () => {
  let connection

  afterEach(async () => {
    try {
      await connection?.close()
    } catch {}

    try {
      await exec(`docker stop ${CONTAINER_NAME}`)
    } catch {}

    try {
      await exec(`docker rm ${CONTAINER_NAME}`)
    } catch {}
  })

  test('connector waits for connection with retry', async () => {
    await startRabbitMQ()
    connection = await AmqpConnector.connect(AMQP_URL, CONNECT_RETRIES, CONNECT_RETRY_DELAY_MS)

    const channel = await connection.createChannel()
    const reply = await channel.assertExchange('test-exchange', 'topic')
    expect(reply.exchange).toEqual('test-exchange')
  }, TEST_TIMEOUT)

  test('channel survives channel error', async () => {
    await startRabbitMQ()
    connection = new AmqpConnection(AMQP_URL, CONNECT_RETRIES, CONNECT_RETRY_DELAY_MS)
    const channel = await connection.createChannel()

    try {
      await channel.checkExchange('test-exchange')
    } catch (error) {
      expect(error).toBeDefined()
    }

    const reply = await channel.assertExchange('test-exchange', 'topic')
    expect(reply.exchange).toEqual('test-exchange')
  }, TEST_TIMEOUT)

  test('channel survives RabbitMQ restart', async () => {
    await startRabbitMQ()
    connection = new AmqpConnection(AMQP_URL, CONNECT_RETRIES, CONNECT_RETRY_DELAY_MS)
    const channel = await connection.createChannel()

    const createExchange = await channel.assertExchange('test-exchange', 'topic')
    expect(createExchange.exchange).toEqual('test-exchange')

    await exec(`docker stop ${CONTAINER_NAME}`)

    await expect(channel.publish('test-exchange', 'test.publish', Buffer.from('Test message'))).rejects.toBeDefined()

    await exec(`docker start ${CONTAINER_NAME}`)

    await expect(channel.publish('test-exchange', 'test.publish', Buffer.from('Test message'))).resolves.toEqual(true)
  }, TEST_TIMEOUT)

  test('publish and consume', async () => {
    expect.assertions(3)

    await startRabbitMQ()
    connection = new AmqpConnection(AMQP_URL, CONNECT_RETRIES, CONNECT_RETRY_DELAY_MS)
    const channel = await connection.createChannel()

    await channel.assertExchange('test-exchange', 'topic')
    await channel.assertQueue('test-queue')
    await channel.bindQueue('test-queue', 'test-exchange', 'test.publish')
    await channel.consume('test-queue', msg => {
      expect(msg).toBeDefined()
      expect(msg.content.toString()).toEqual('Test message')
    })

    await expect(channel.publish('test-exchange', 'test.publish', Buffer.from('Test message'))).resolves.toEqual(true)
    await sleep(PUBLICATION_DELAY_MS)
  }, TEST_TIMEOUT)

  test('publish and consume with async handler', async () => {
    expect.assertions(3)

    await startRabbitMQ()
    connection = new AmqpConnection(AMQP_URL, CONNECT_RETRIES, CONNECT_RETRY_DELAY_MS)
    const consumerChannel = await connection.createChannel()

    await consumerChannel.assertExchange('test-exchange', 'topic')
    await consumerChannel.assertQueue('test-queue')
    await consumerChannel.bindQueue('test-queue', 'test-exchange', 'test.publish')
    await consumerChannel.consume('test-queue', async msg => {
      expect(msg).toBeDefined()
      expect(msg.content.toString()).toEqual('Test message')
      throw new Error('Test')
    })

    const publisherChannel = await connection.createChannel()
    const publishResult = publisherChannel.publish('test-exchange', 'test.publish', Buffer.from('Test message'))
    await expect(publishResult).resolves.toEqual(true)
    await sleep(PUBLICATION_DELAY_MS)
  }, TEST_TIMEOUT)

  test('consumer survives RabbitMQ restart', async () => {
    expect.assertions(4)

    await startRabbitMQ()
    const connectionLossHandler = jest.fn()
    connection = new AmqpConnection(AMQP_URL, CONNECT_RETRIES, CONNECT_RETRY_DELAY_MS, connectionLossHandler)
    const consumerChannel = await connection.createChannel()

    await consumerChannel.assertExchange('test-exchange', 'topic')
    await consumerChannel.assertQueue('test-queue')
    await consumerChannel.bindQueue('test-queue', 'test-exchange', 'test.publish')
    await consumerChannel.consume('test-queue', msg => {
      expect(msg).toBeDefined()
      expect(msg.content.toString()).toEqual('Test message')
    })

    await exec(`docker kill ${CONTAINER_NAME}`)
    await exec(`docker start ${CONTAINER_NAME}`)

    const publisherChannel = await connection.createChannel()
    const publishResult = publisherChannel.publish('test-exchange', 'test.publish', Buffer.from('Test message'))
    await expect(publishResult).resolves.toEqual(true)
    await sleep(PUBLICATION_DELAY_MS)
    expect(connectionLossHandler).toBeCalledTimes(0)
  }, TEST_TIMEOUT)

  test('connection loss handler is called', async () => {
    await startRabbitMQ()
    const connectionLossHandler = jest.fn()
    connection = new AmqpConnection(AMQP_URL, CONNECT_RETRIES, CONNECT_RETRY_DELAY_MS, connectionLossHandler)
    const consumerChannel = await connection.createChannel()

    await consumerChannel.assertExchange('test-exchange', 'topic')
    await consumerChannel.assertQueue('test-queue')
    await consumerChannel.bindQueue('test-queue', 'test-exchange', 'test.publish')
    await consumerChannel.consume('test-queue', msg => {
      expect(msg).toBeDefined()
      expect(msg.content.toString()).toEqual('Test message')
    })

    // Kill RabbitMQ to trigger a connection error
    // A stop does not work, because it results in an orderly connection close
    await exec(`docker kill ${CONTAINER_NAME}`)
    await sleep(30000) // 5 retries with each 5000 delay plus some safety delay
    expect(connectionLossHandler).toBeCalledTimes(1)
  }, TEST_TIMEOUT)
})
