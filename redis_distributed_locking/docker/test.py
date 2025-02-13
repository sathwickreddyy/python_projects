import threading, logging
from application import ApplicationLogic
from config import  LocalConfigManager
from leader_election import LeaderElection

if __name__ == '__main__':
    application_logic = ApplicationLogic()
    config_manager = LocalConfigManager(".env")

    leader_election = LeaderElection("Api", config_manager)

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