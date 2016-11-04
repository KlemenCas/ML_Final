**DEMO:**
In demo mode only data to the SP500 sector 'Telecommunications Services' will be loaded and processed. Demo flag is being set in commons.py - see variable _demo_scenario_. Otherwise all stock symbols will be processed (500 stocks!), and it will take a long time...

**Data Retrieval:**

1. For data retrieval please run the python module _mydata\_get\_delta.py_. 
2. Quandl Key needs to be provided.
3. Subscription to 2 databases is required: SP0 (needs subscription, but is free) and SF1 (not free!) 


**Training:**
For training please run _train.py_. Note!: the l_pca needs to be set to the dimension reduction that you want to train. Currently it's set to 0 (= no reduction)
