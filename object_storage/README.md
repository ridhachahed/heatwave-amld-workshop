# Create Object Storage bucket and upload files

* Go to *Menu* > *Storage* > *Buckets*.
![step_1](images/os_step_1.png)

* Click *Create Bucket*.
![step_2](images/os_step_2.png)

* Change the bucket name (e.g. `mysql-hw-bucket`), leave everything else by default. Click *Create*.
![step_3](images/os_step_3.png)

* Click on your new bucket to see details.
![step_4](images/os_step_4.png)

* On the section *Objects*, click *Upload*.
![step_5](images/os_step_5.png)

* Download the dataset [bank_marketing](https://github.com/oracle-samples/heatwave-ml?tab=readme-ov-file) (link in the README) and unzip it.

* Drop the unzipped `bank-full.csv` file on *Choose Files from your Computer*. Leave everything else by default and click *Upload*, then *Close*.
![step_6](images/os_step_6.png)

* You will see the new file in the Objects list. Click on the three dots contextual menu and select *View Object Details*.
![step_7](images/os_step_7.png)

* Take note of the URL you have. We will need it later.
![step_8](images/os_step_8.png)

As you move forward, the next step in enhancing your cloud infrastructure involves creating a bastion node. This bastion node will serve as a secure gateway, enabling you to connect to your MySQL database system from outside the Oracle Cloud network. By establishing this connection, you'll ensure that you can manage and interact with your database securely, paving the way for more advanced configurations and operations.