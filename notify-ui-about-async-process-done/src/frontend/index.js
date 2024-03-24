import { Amplify } from 'aws-amplify';
import config from './aws-exports.js';
import {generateClient} from 'aws-amplify/api';
import {startProcess} from "./src/graphql/mutations.js";
import {onProcessDone} from "./src/graphql/subscriptions.js";

Amplify.configure(config);

class App {
  constructor(processIdInputFieldId, messagesDivId, startButtonId) {
    console.log('App instantiated');
    this.processIdInputField = document.getElementById(processIdInputFieldId);
    this.messagesDiv = document.getElementById(messagesDivId);
    this.startButton = document.getElementById(startButtonId);
    this.startButton.onclick = async () => {
      const processId = this.processIdInputField.value;
      console.log('Starting process:', processId);
      await this.startProcess(processId);
    }
    this.client = generateClient();
    window.addEventListener('beforeunload', () => {
      if (this.subscription) {
        this.subscription.unsubscribe();
      }
    })
  }

  async startProcess(processId) {
    try {
      const startProcessResult = await this.client.graphql({
        query: startProcess,
        variables: {
          process_id: processId
        }
      })
      this.messagesDiv.innerHTML += `<div>Process started: ${processId}</div>`;
      this.messagesDiv.innerHTML += `<div>${JSON.stringify(startProcessResult.data)}</div>`
      console.log('Process started:', startProcessResult);
      await this.subscribeToProcessDone(processId);
    } catch (error) {
      console.error('Error starting process:', error);
    }
  }

  async subscribeToProcessDone(processId) {
    const subscription = this.client.graphql({
      query: onProcessDone,
        variables: {
          id: processId
        }
    }).subscribe({
      next: (eventData) => {
        const message = eventData.data;
        this.messagesDiv.innerHTML += `<div>${JSON.stringify(message)}</div>`;
        subscription.unsubscribe();
        this.messagesDiv.innerHTML += `<div>Subscription to ${processId} deleted</div>`;
      },
      error: (error) => console.error(error)
    });
    this.messagesDiv.innerHTML += `<div>Subscription to ${processId} created</div>`
    this.subscription = subscription;
  }
}
window.App = App;
