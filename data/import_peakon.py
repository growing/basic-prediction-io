"""
Import sample data for recommendation engine
"""

import predictionio
import argparse
import random

RATE_ACTIONS_DELIMITER = "\t"
SEED = 3

properties = ["Manager","Team","Office"]

def import_events(client, file):
  f = open(file, 'r')
  random.seed(SEED)
  count = 0
  print "Importing data..."
  for line in f:
    data = line.rstrip('\r\n').split(RATE_ACTIONS_DELIMITER)
    # For demonstration purpose, randomly mix in some buy events
    # For the MMR add some item metadata

    if (data[0] == "ITEM"):
        client.create_event(
          event="$set",
          entity_type="item",
          entity_id=data[1],
          properties= { "category": [data[2]] }
        )
    else:
        if data[1] in properties:
            #prop = data[1]
            client.create_event(
              event="$set",
              entity_type="user",
              entity_id=data[0],
              properties= { data[1]: data[2] }
            )
        else:
            if ((int(data[0]) > 200) and (random.random() < 0.5)):
                print "skipping " + data[0] + " question " + data[1]
            else:
                client.create_event(
                    event="rate",
                    entity_type="user",
                    entity_id=data[0],
                    target_entity_type="item",
                    target_entity_id=data[1],
                    properties= { "rating": int(data[2]) }
                 )

    count += 1

    print "%s events are imported." % count
  f.close()


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for recommendation engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/se.csv")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)
