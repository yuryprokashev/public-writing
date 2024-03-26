import { Amplify } from 'aws-amplify';
import config from './aws-exports.js';
import {generateClient} from 'aws-amplify/api';
import {startProcess, startProcessLambda} from "./src/graphql/mutations.js";
import {signIn, signOut, getCurrentUser, fetchUserAttributes} from 'aws-amplify/auth';
const getRequestHeaders = async () => {
    const userAttributes = await fetchUserAttributes()
    return {
      'x-usage-plan-id': userAttributes['custom:usage_plan']
    }
}

const getLambdaAuthToken = async() => {
  const user = await getCurrentUser()
  return `user_id=${user.username}`
}



class App {
  constructor({
                processInputId,
                messagesId,
                startCognitoProcessButtonId,
                startLambdaProcessButtonId,
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
    this.startButtonCognito = document.getElementById(startCognitoProcessButtonId);
    this.startButtonLambda = document.getElementById(startLambdaProcessButtonId);
    this.usernameInputField = document.getElementById(usernameInputId);
    this.passwordInputField = document.getElementById(passwordInputId);
    this.initStartProcessCognitoButton();
    this.initStartProcessLambdaButton();
    this.initSignInButton();
    this.initSignOutButton();
    Amplify.configure(config, {
      API: {
        GraphQL: {
          headers: getRequestHeaders
        }
      }
    });
    this.cognitoClient = generateClient();
    this.lambdaClient = generateClient({authMode: 'lambda'})
  }
  initStartProcessCognitoButton() {
    this.startButtonCognito.onclick = async () => {
      const processId = this.processIdInputField.value;
      console.log('Starting process:', processId);
      await this.startProcess(processId, this.startProcessCognito.bind(this));
    }
  }
  initStartProcessLambdaButton() {
    this.startButtonLambda.onclick = async () => {
        const processId = this.processIdInputField.value;
        console.log('Starting process:', processId);
        await this.startProcess(processId, this.startProcessLambda.bind(this));
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

  async startProcess(processId, fetcher) {
    try {
      const startProcessResult = await fetcher(processId);
      this.messagesDiv.innerHTML += `<div>Process started: ${processId}</div>`;
      this.messagesDiv.innerHTML += `<div>${JSON.stringify(startProcessResult.data)}</div>`
      console.log('Process started:', startProcessResult);
    } catch (error) {
      console.error('Error starting process:', error);
    }
  }
  async startProcessCognito(processId) {
    return this.cognitoClient.graphql({
        query: startProcess,
        variables: {
          process_id: processId
        }
      })
  }
  async startProcessLambda(processId) {
    const token = await getLambdaAuthToken()
    return this.lambdaClient.graphql({
        query: startProcessLambda,
        variables: {
          process_id: processId
        },
        authToken: token
      })
  }
}
window.App = App;
