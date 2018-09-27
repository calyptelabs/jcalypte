# Calypte

Calypte é uma ótima opção de cache para aplicações Java. Ele é extremamente rápido, trabalha de forma eficiente com pouca memória, permite o armazenamento de dados em memória secundária e tem suporte transacional.

## Instalação

O Calypte pode ser instalado usando o Maven ou clonando o projeto e configurando-o no classpath da aplicação.

### Clonando o projeto

Se for clonar o projeto, siga os seguintes passos:

1. git clone https://github.com/calyptelabs/jcalypte.git;
2. mvn clean install;
3. configurar o maven.

### Maven

Se for utilizado o Maven, deve ser informado no pom.xml:

```
<dependency>
    <groupId>calypte</groupId>
    <artifactId>jcalypte</artifactId>
    <version>1.0.0.0</version>
</dependency>
```
