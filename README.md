This SMT reverses the flattening of the document.

Properties:

Example on how to add to your connector:

```
transforms=unflatten
transforms.unflatten.delimiter=.
transforms.unflatten.type=com.github.pushupek.kafka.connect.smt.Unflatten$Value
```
