# BtDatabase
### A Java library to make use of the JDBC API via intuitive method chaining. 

**This library is not a full on DBMS.** It uses the [Apache Derby DB](https://db.apache.org/derby/) under the hood and should be seen as an additional bridge between you and the JDBC API to allow intuitive method chaining while also adding some useful utility features such as trigger listeners and automated persisting of entire objects.

### Contents
- [How to get started]()
  - [Requirements]()


## How to get started

#### Requirements
- Java 11+


#### Add BtDatabase via Maven
To add this library to your project via Maven simply add the follosing lines to your pom.xml file.

```xml
<repositories>
        <repository>
            <id>jitpack.io</id>
            <url>https://jitpack.io</url>
        </repository>
    </repositories>
  <dependencies>
    <dependency>
        <groupId>com.github.Bowtie8904</groupId>
        <artifactId>BtDatabase</artifactId>
        <version>1.0</version>
    </dependency>
  </dependencies>
``` 
