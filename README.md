# TP DMB : spark

Dans ce TP nous avons utilisé spark pour résoudre des relations entre plusieurs attributs d'un dataset.

Nous avons choisis d'utiliser l'api python pour répondre aux questions.

L'utilisation de cette api no sql est plus lisible qu'un code sql qui aurait pu avoir le meme effet.

La librairie est très bien documentée et permet de faire des requetes complexes, tout en gardant une certaine lisibilité pour l'humain.

Le code se situe à la racine du repo github séparé en partie dans différents fichiers només PartX. 


## Comparaison des score des premiers joueurs 

Voila le tableau des premiers joueurs basés sur leur position dans les parties joués.

```
+---------------+----------------+---------------+
|    player_name|average_position|number_of_games|
+---------------+----------------+---------------+
|       ChanronG|             9.0|              4|
|  JustTuatuatua|           10.75|              4|
|       dman4771|            11.5|              4|
|         KBSDUI|            12.0|              4|
|      TemcoEwok|           13.25|              4|
|     PapaNuntis|           13.25|              4|
|        Dcc-ccD|            14.5|              4|
|China_huangyong|           21.75|              4|
|   siliymaui125|           22.75|              4|
|      crazyone8|           23.25|              4|
+---------------+----------------+---------------+
```

Et voici la table des dix premiers joueurs basé sur leur nombre de kill

```
+--------------+-------------+---------------+
|   player_name|average_kills|number_of_games|
+--------------+-------------+---------------+
|LawngD-a-w-n-g|          2.2|              5|
|  siliymaui125|          2.0|              4|
|       Dcc-ccD|         1.75|              4|
|      dman4771|         1.75|              4|
|     NerdyMoJo|          1.5|              4|
|   Roobydooble|          1.0|              4|
|    PapaNuntis|          1.0|              4|
| JustTuatuatua|         0.75|              4|
|       GenOrgg|          0.5|              4|
|      ChanronG|          0.5|              4|
+--------------+-------------+---------------+
```

On peut retrouver certains joueurs dans les deux classements. 
Ainsi, on peut spontanement répondre que faire des kills semblerait assurer un classement plus haut 



