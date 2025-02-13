import threading, logging
from application import ApplicationLogic
from config import SSMConfigManager
from leader_election import LeaderElection
from sns_notifier import SNSNotifier

if __name__ == '__main__':
    application_logic = ApplicationLogic()
    config = SSMConfigManager()
    sns_topic_arn = config.get_parameter("/app/config/sns_topic_arn")
    sns_notifier = SNSNotifier(sns_topic_arn)

    print("Starting Program")
    leader_election = LeaderElection("Api", config)

    leader_election.elect_leader()
    response = ""
    if leader_election.i_am_leader():
        try:
            # create a thread which sends heartbeat every five seconds
            threading.Thread(target=leader_election.send_heartbeats, daemon=True).start()
            application_logic.execute()
        except Exception as e:
            logging.error(f"Leader encountered an error during execution: {e}")
        finally:
            leader_election.cleanup()
    else:
        # followers must wait for leader to complete execution
        leader_election.monitor_leader()