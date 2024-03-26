import { Amplify } from 'aws-amplify';
import config from './aws-exports.js';
import {generateClient} from 'aws-amplify/api';
import {startProcess} from "./src/graphql/mutations.js";
import {signIn, signOut, getCurrentUser, fetchUserAttributes} from 'aws-amplify/auth';



class App {
  constructor({
                processInputId,
                messagesId,
                startProcessButtonId,
                signInButtonId,
                signOutButtonId,
                usernameInputId,
                passwordInputId
  }) {
    console.log('App instantiated');
    this.signInButton = document.getElementById(signInButtonId);
    this.signOutButton = document.getElementById(signOutButtonId);
    this.processIdInputField = document.getElementById(processInputId);
    this.messagesDiv = document.getElementById(messagesId);
    this.startButton = document.getElementById(startProcessButtonId);
    this.usernameInputField = document.getElementById(usernameInputId);
    this.passwordInputField = document.getElementById(passwordInputId);
    this.initProcessButton()
    this.initSignInButton()
    this.initSignOutButton()
    Amplify.configure(config);
    this.client = generateClient();
  }
  initProcessButton() {
    this.startButton.onclick = async () => {
      const processId = this.processIdInputField.value;
      console.log('Starting process:', processId);
      await this.startProcess(processId);
    }
  }
  initSignInButton() {
    this.signInButton.onclick = async () => {
      try {
        await signIn({
          username: this.usernameInputField.value,
          password: this.passwordInputField.value
        });
        console.log('User signed in');
        const user = await getCurrentUser();
        console.log('user', user);
        const userAttributes = await fetchUserAttributes(user);
        console.log('userAttributes', userAttributes);
      } catch (error) {
        console.error('Error signing in:', error);
      }
    }
  }
  initSignOutButton() {
    this.signOutButton.onclick = async () => {
      try {
        await signOut();
        console.log('User signed out');
      } catch (error) {
        console.error('Error signing out:', error);
      }
    }
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
    } catch (error) {
      console.error('Error starting process:', error);
    }
  }
}
window.App = App;
