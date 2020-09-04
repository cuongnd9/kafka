import { name } from 'faker';
import delay from 'delay';

import { subscribe } from './pkg/consumer';
import { publish } from './pkg/producer';

(async () => {
  const TOPIC = 'MEOW_MOEW';

  publish(TOPIC, name.jobTitle());
  await delay(1234);
  subscribe(TOPIC);
})();
