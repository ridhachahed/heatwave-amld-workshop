# Setting Up the Bastion Host and MySQL Shell

Welcome to the second part of our workshop. This section is dedicated to setting up a Bastion Host, an essential component in managing secure access to your cloud resources. Follow the steps below to configure and utilize a Bastion Host effectively.


## Overview 

A Bastion Host serves as a gateway to securely access your private cloud resources from an external network. It acts as a single entry point, reducing the potential attack surface and thereby enhancing your cloud environment's security. The Bastion Host is typically placed in a public subnet but is secured with stringent access rules.


## Objectives 

- Understand the role and importance of a Bastion Host in a secure cloud architecture.
- Learn how to configure a Bastion Host in your Virtual Cloud Network (VCN).


## Prerequisites 

- A functioning OCI environment with a configured VCN.
- Necessary IAM permissions to create and manage compute instances.


## Understanding Key Concepts

Before we proceed with the configuration of the Bastion Host, it’s crucial to grasp some key concepts that underline its importance and functionality in securing cloud environments.

### What is a Bastion Host?
**Definition**: A Bastion Host is a specialized server designed to provide secure access to private network resources from an external network, typically the internet. It acts as a 'jump server' or 'jump host', allowing secure SSH or RDP connections to be made to other internal resources.

**Purpose**: The primary role of the Bastion Host is to limit the exposure of internal resources to external threats, serving as a single, monitored, and controlled access point.


## Step-by-Step Guide


### Step 1: Access the Instances Section

- Once logged in, go to the main dashboard.
- Navigate to the **Compute** section and select **"Instances"**.

![](images/get_instance.png)


### Step 2: Provisioning the Host

- Click on **"Create Instance"**.

- Navigate to the **Compute** section and select **"Instances"**.

![](images/create_instance.png)

- Set the name of the instance as **bastion**.

![](images/name_bastion.png)

- By scrolling down you check the Image and Shape details 
- For the Networking information make sure that the VCN selected is the one we created in the previous step i.e.**"analytics_vcn"** with the **"public subnet-analytics_vcn"**.

![](images/vcn_settings.png)

- Scroll down to the Add SSH Key.
- Click on the **"Save private key"** button provided. The private key should be a .key  file.

The file you are downloading is crucial for securely accessing the Bastion Host. It acts as a cryptographic key that uniquely authenticates your SSH session. You will use the private key later on to connect your compute instance.


- After downloading the private key, you initiate the Bastion Host provisioning by cliking on **"Create"**.

![](images/key_save.png)

After few seconds, you will be able to see that the Bastion Host instance has a **running** status. 

- Make sure to take of the **Public IP Address** as you will need it to connect to the host 

![](images/bastion_running.png)


### Step 3: Connect to the Bastion Host using the Cloud Shell

To establish a connection with the bastion host, we'll utilize the cloud shell, which is a compact Linux terminal integrated into the OCI interface. You can access the cloud shell by clicking on the shell icon located adjacent to the OCI region name, at the top right corner of the page.

![](images/cloud_shell.png)


- Establish an SSH connection with the bastion host using its Public IP address by exectuing the following commands in your cloud shell :
```
chmod 600 <private-key-file-name>.key
ssh -i <private-key-file-name>.key opc@<compute_instance_public_ip>
```

If prompted to accept the finger print, enter _**yes**_ and hit enter.

![](images/bastion_connection.png)

### Step 4: 

If you install MySQL Shell on the bastion host, you can then use it to connect securely to your MySQL database server residing on a different machine in your private network. This setup is useful if you want to prevent direct exposure of your MySQL server to the internet and allow database connections only through the bastion.


- You can now install MySQL Shell executing the following commands :

```
sudo yum install mysql-shell  
```

- Launch MySQL Shell executing the following command:
```
mysqlsh
```
When you see the MySQL Shell colorful prompt, exit with the following command:
```
\q
```

![](images/mysqlsh.png)


## Conclusion
You have successfully set up a compute instance to function as a bastion host, establishing it as the public gateway to the Virtual Cloud Network (VCN) we crafted earlier. You have streamlined a direct connection to your Database System through
the bastion node. We're now ready to move forward and initiate the deployment of the MySQL Database Service (MDS) with Heatwave in our upcoming lab!





