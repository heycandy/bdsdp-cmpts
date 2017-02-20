# Ark-BDADP #
中软国际大数据场景开发平台

## 组件开发 ##

**1. 基本目录结构**

```

--src
  --main
    --java
    --resources
      --icon
        --icon-xl.png
        --icon-xs.png
      --config.xml
      --i18n_en.properties
      --i18n.zh.properties
    --scala
  --test
    --java
    --scala

```

**1. 需要自动将JAR包复制到父目录的**

```

<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>bdsdp-cmpts</artifactId>
        <groupId>com.chinasofti.ark</groupId>
        <version>1.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>cmpt-content-conversion</artifactId>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-antrun-plugin</artifactId>
            </plugin>
        </plugins>
    </build>

</project>

```

**2. 需要有第三方依赖的**

```

<?xml version="1.0" encoding="UTF-8"?>
<project xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xmlns="http://maven.apache.org/POM/4.0.0"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>bdsdp-cmpts</artifactId>
        <groupId>com.chinasofti.ark</groupId>
        <version>1.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>cmpt-sample-scala</artifactId>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>

```

**3. 需要提供Scala编译支持**

```

<?xml version="1.0" encoding="UTF-8"?>
<project xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xmlns="http://maven.apache.org/POM/4.0.0"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>bdsdp-cmpts</artifactId>
        <groupId>com.chinasofti.ark</groupId>
        <version>1.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>cmpt-sample-scala</artifactId>

    <build>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>

```

----

## 工程打包 ##

**1. Maven**

     mvn package

**2. IDEA**

   点击右侧工具栏的Maven Projects，选择root目录->Lifecycle->package

**特别注意**

    本项目依赖ark-bdadp,开发前请先确认ark-bdadp已安装至本地仓库，如：mvn install -P war
    
----



