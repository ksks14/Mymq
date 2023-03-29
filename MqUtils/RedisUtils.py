import asyncio
import aioredis


class Redis:
    def __init__(self, config: dict = None):
        self.connection_url = f'redis://localhost'

    async def create_connect(self, ):
        """

        :return:
        """
        self.connnection = aioredis.from_url(self.connection_url)
        return self.connnection


class Producer:
    def __init__(self, redis_client):
        """

        :param redis_client:
        """
        self.redis_client = redis_client
        
    async def add_to_stream(self, data, stream_channle):
        """

        :param data:
        :param stream_channle:
        :return:
        """
        try:
            msg_id = await self.redis_client.xadd(name=stream_channle, id='*', fields=data)
            return msg_id
        except Exception as e:
            raise Exception(f"Error sending msg to stream -> {e}")


class StreamConsumer:
    def __init__(self, redis_client):
        """

        :param redis_client:
        """
        self.redis_client = redis_client

    async def consume_stream(self, count: int, block: int, stream_channel):
        """

        :param block:
        :param count:
        :param stream_channel:
        :return:
        """
        response = await self.redis_client.xread(streams={stream_channel: '0-0'}, count=count, block=block)

        return response

    async def delete_message(self, stream_channel, message_id):
        """

        :param stream_channel:
        :param message_id:
        :return:
        """
        await self.redis_client.xdel(stream_channel, message_id)


async def main():
    redis_conn = await Redis().create_connect()
    produce = Producer(redis_conn)
    consumer = StreamConsumer(redis_conn)
    # 添加一个消息到队列中
    data = {'xiaoming4': 123}
    await produce.add_to_stream(data=data, stream_channle='message_channel')

    # 从队列中拿出最新的1条数据
    data = await consumer.consume_stream(1, block=0, stream_channel='message_channel')
    print(data)

    # 轮询等待队列中的新消息
    response = await consumer.consume_stream(stream_channel="message_channel", count=1, block=0)
    if response:
        for stream, messagees in response:
            print('stream:', stream)
            for message in messagees:
                print('message: ', message)
                message_id = message[0]
                print('message_id: ', message_id)
                message_content = message[1]
                print('message_content: ', message_content)
                print('注意里面的键、值都变成了byte类型，需要进行解码:')
                message_content: dict
                print('message_content_decode: ',
                      {k.decode('utf-8'): v.decode('utf-8') for k, v in message_content.items()})

    # 消费成功后删除队列中的消息
    await consumer.delete_message(
        stream_channel='message_channel', message_id=message_id
    )


if __name__ == '__main__':
    asyncio.run(main())
