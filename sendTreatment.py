import sys
import json
import pandas as pd
import QuijoteMethods

if len(sys.argv) < 2:
    print("General Error: No configuration file provided")
else:
    configJson = sys.argv[1]

    with open(configJson,'r') as infile:
        pushConfig = json.load(infile)

    if pushConfig.get('env') == 'stg':
        QuijoteMethods.CURR_ENV = QuijoteMethods.ENV_STG

    if pushConfig.get('file'):
        pushTargets = pd.read_csv(pushConfig.get('file'))
    else:
        print("Configuration Error: No Input File");

    pushTargets['channel'] = pushConfig.get('channel')
    pushTargets['param'] = str(pushConfig.get('param'))
    pushTargets['trigger'] = pushConfig.get('trigger')

    pushTargets = pushTargets[['channel', 'user_id', 'uaid', 'param', 'trigger']]

    result = QuijoteMethods.sendBulkRequest(pushTargets)

    if pushConfig.get('append_output'):
        mode = 'a'
    else:
        mode = 'w'

    if pushConfig.get('outfile'):
        with open(pushConfig.get('outfile'), mode) as outfile:
            outfile.writelines(str(result))
    else:
        print(result)
