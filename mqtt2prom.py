import asyncio
import json
import logging
import re
import signal

import httpcore
from httpcore import ConnectError

from config import get_first_config
from mqtt_handler import MqttHandler

__version__ = '0.0.5'


class Mqtt2Prom:
    def __init__(self):
        self.config = get_first_config()
        self.setup_logging()
        self.metric_url = self.config['prom_import_url']
        self.mqtt_handler: MqttHandler = MqttHandler(self.config['mqtt'], self.handle_mqtt_message)

    def setup_logging(self):
        if 'logging' in self.config:
            logging_level_name: str = self.config['logging'].upper()
            logging_level: int = logging.getLevelNamesMapping().get(logging_level_name, logging.NOTSET)
            if logging_level != logging.NOTSET:
                logging.getLogger().setLevel(logging_level)
            else:
                logging.warning(f'unknown logging level: %s.', logging_level)

    async def handle_mqtt_message(self, topic: str, payload: str):
        logging.debug('handle topic: %s, payload: %s', topic, payload)
        topic_options = self.config['mqtt']['topics'].get(topic, {})
        if not topic_options:
            for config_topic, topic_data in list(self.config['mqtt']['topics'].items()):
                if 'regex' not in topic_data:
                    continue
                pattern = re.escape(config_topic).replace(r'\+', r'([^/]+)')
                match = re.fullmatch(pattern, topic)
                if not match:
                    continue
                topic_options = {k: v for k, v in topic_data.items() if k != 'regex'}
                topic_options['metric_name'] = config_topic.replace('/+', '').replace('/', '_')
                topic_options['label'] = f'{{{topic_data["regex"]}="{match.group(1)}"}}'
                self.config['mqtt']['topics'][topic] = topic_options
        if not topic_options:
            logging.warning(f'unknown!: {topic}:{payload}')
        topic_type = topic_options.get('type', 'raw')
        topic_label = topic_options.get('label', '')
        metric_name = topic_options.get('metric_name', topic).replace('/', '_')
        if topic_type == 'raw':
            await self.send_metric(metric_name, payload, topic_label)
        elif topic_type == 'json':
            data = json.loads(payload)
            json_filter = topic_options.get('json_filter')
            if json_filter:
                data_override = {}
                for key in json_filter:
                    value = data
                    for part in key.split('.'):
                        if not isinstance(value, dict) or part not in value:
                            break
                        value = value[part]
                    else:
                        data_override[key.replace('.', '_')] = value
                data = data_override
            for key, value in data.items():
                await self.send_metric(f'{metric_name}_{key}', value, topic_label)

    async def send_metric(self, metric_name: str, value: str, label: str) -> str:
        content = f'{metric_name}{label} {value}\n'
        if not self.metric_url:
            logging.debug('send metric: %s', content.strip())
            return ''
        async with httpcore.AsyncConnectionPool() as http:
            try:
                response = await http.request(
                    method='POST',
                    url=self.metric_url,
                    content=content.encode()
                )
                return response.content.decode()
            except ConnectError as e:
                logging.warning(f'{e=}, {content=}')
            except Exception as e:
                logging.error(f'{e=}, {content=}')
            return ''

    async def exit(self):
        await self.mqtt_handler.disconnect()


async def main():
    mqtt2prom = Mqtt2Prom()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def shutdown_handler():
        stop_event.set()

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_handler)
    except NotImplementedError:
        pass
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logging.info('exiting.')
    finally:
        await mqtt2prom.exit()
        logging.info('exited.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('gmqtt').setLevel(logging.ERROR)
    logging.info(f'starting Mqtt2Prom v%s.', __version__)
    asyncio.run(main())
