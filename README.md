# WayToFreedom
WayToFreedom project formal repository.

## Description
WayToFreedom is an AWS based project, and it has two parts of work:
* Code in github
* Configurations in AWS
Eventually, all the code and configurations should be stored in github. Configurations should be a CloudFormation format. However, in short this is still the realistic solution.

Now everything except environment configuration should be checked in through git and reviewed by others.

## Deployment
Now the deployment happened in a tricky way. Everything for each component should be packed in a package and then uploaded to AWS lambda function.

WTFBuyingSignalComponent:
* BuyingSignalComponent.py
* holdingStatusTable.py
* SellingSignalComponent.py
* tradingSignalHistoryTable.py

WTFSellingSignalComponent:
* SellingSignalComponent.py
* holdingStatusTable.py
* SellingSignalComponent.py
* tradingSignalHistoryTable.py

## Todo
