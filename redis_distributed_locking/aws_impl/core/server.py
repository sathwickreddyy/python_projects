import time
from flask import Flask, jsonify
import threading
from application import ApplicationLogic
from config import SSMConfigManager
from leader_election import LeaderElection
from sns_notifier import SNSNotifier

app = Flask(__name__)

@app.route('/execute', methods=['POST'])
def execute():
    try:
        application_logic = ApplicationLogic()
        config = SSMConfigManager()
        sns_topic_arn = config.get_parameter("/app/config/sns_topic_arn")
        sns_notifier = SNSNotifier(sns_topic_arn)

        leader_election = LeaderElection("some_api_call", config)
        leader_election.elect_leader()
        response = ""
        if leader_election.i_am_leader():
            try:
                # create a thread which sends heartbeat every five seconds
                threading.Thread(target=leader_election.send_heartbeats, daemon=True).start()
                application_logic.execute()
                sns_notifier.send_message("Leader notification", f"Leader successfully completed execution of critical sections.")
            except Exception as e:
                sns_notifier.send_message("Execution Error", f"Leader encountered an error during execution: {e}")
                return jsonify({
                    "status": "error",
                    "message": str(e)
                }), 400
            finally:
                leader_election.cleanup()
            # clean up after execution.
            leader_election.cleanup()
        else:
            # followers must wait for leader to complete execution
            leader_election.monitor_leader()
            time.sleep(3)
            sns_notifier.send_message("Follower Notification", "Follower did not execute critical sections. & process Completed.")

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
