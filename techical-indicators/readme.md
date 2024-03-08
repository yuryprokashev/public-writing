# Getting Started with Technical Indicators
## Requirements
- Python 3.11
- CDK 2.110 or higher (see [Getting Started with CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html))

## Usage
- Check out the code
- Open Terminal and navigate to the project folder
- Create a virtual environment: `python -m venv .venv`
- Activate the virtual environment: `source .venv/bin/activate`
- Install dependencies: `pip install -r requirements.txt`
- Deploy the stack first time: `cdk deploy -c`
- Deploy the stack once the layer with TA-Lib and other dependencies is ready: `cdk deploy -c talibLayerArn=<YOUR_LAYER_ARN>`
