# PyJobHunter
 For now, its an async Python web "collector" that *basically* takes a job title & Resume (and CL if you got it) as an input and returns a ranked list of jobs best suited.


::: mermaid
graph LR;
    UserInput:JobTitle-->CalculateMaxPages;
    CalculateMaxPages-->GenerateURLS;
:::