import time
from flask import Flask, jsonify
import threading
from application import ApplicationLogic
import logging

from docker.config import LocalConfigManager
from leader_election import LeaderElection

app = Flask(__name__)

@app.route('/execute', methods=['POST'])
def execute():
    try:
        application_logic = ApplicationLogic()
        config_manager = LocalConfigManager(".env")

        leader_election = LeaderElection("Api", config_manager)

        leader_election.elect_leader()
        response = ""
        print("Starting Program")
        if leader_election.i_am_leader():
            try:
                print("No Leader or executing, becoming leader")
                # create a thread which sends heartbeat every five seconds
                leader_election.start_heartbeat_thread()
                application_logic.execute()
                response = "Leader successfully completed execution of critical sections. \n" + response
            except Exception as e:
                logging.error(f"Leader encountered an error during execution: {e}")
                print(f"Leader encountered an error during execution: {e}")
                return jsonify({
                    "status": "error",
                    "message": str(e)
                }), 500
            finally:
                leader_election.cleanup()
        else:
            print("Already an instance/leader or executing, becoming follower of the leader")
            # followers must wait for leader to complete execution
            response = "Follower completed waiting and no action performed"
            leader_election.monitor_leader()
            time.sleep(2)
        leader_election.cleanup()
        return jsonify({
            "status": "success",
            "output": response,
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    # Start Flask server on port 80
    app.run(host='0.0.0.0', port=80)
