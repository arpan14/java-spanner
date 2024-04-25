## Running

The benchmark application includes PGAdapter as a dependency and automatically starts PGAdapter as
an in-process service.

```shell
mvn clean compile exec:java -Dexec.args="--clients=1 --operations=1000 --multiplexed=true"
```