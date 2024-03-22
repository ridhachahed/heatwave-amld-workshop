# Setup a MySQL HeatWave DB System

1. On the OCI Console, go to the hamburger menu, then choose *Databases* and *DB Systems* under the MySQL HeatWave section
![step_1](images/mysql_hw_step_1.png)

* From this page, click on *Create DB System*.
![step_2](images/mysql_hw_step_2.png)

* Choose *Development or testing*, then give a name to your DB system.
![step_3](images/mysql_hw_step_3.png)

* Scroll down to *Create administrator credentials*, create username and password, and make sure that *Standalone* is selected.
![step_4](images/mysql_hw_step_4.png)

* Scroll down to the *Configure networking* section. Make sure to choose the VCN and `public` subnet you created in the previous Lab. You can leave the *Configure placement* section unchanged.
![step_5](images/mysql_hw_step_5.png)

* Scroll down to the end of the page first, and click on *Show advanced options*. Change to the *Configuration* tab and choose *8.3.0 - Innovation* as MySQL version. Do not click on *Create* yet!
![step_6](images/mysql_hw_step_6.png)

* Scroll back up to the *Configure hardware* section, and make sure that *Enable HeatWave* is checked. First, we change the configuration of the core MySQL node to MySQL.8, as follows:
![step_7_a](images/mysql_hw_step_7_a.png)
![step_7_b](images/mysql_hw_step_7_b.png)

Then, we change the configuration of the HeatWave cluster to have 2 nodes and the HeatWave.512GB shape, as well as enable the Lakehouse functionality.
![step_7_c](images/mysql_hw_step_7_c.png)
![step_7_d](images/mysql_hw_step_7_d.png)

* Finally, scroll down to the *Configure backup plan* section and make sure to `disable` point-in-time recovery, then click on *Create*. Give it a few minutes until all the infrastructure is created for you. Happy HeatWaving!
![step_8](images/mysql_hw_step_8.png)
