

transformar = lambda x,y: x**y

print(transformar(3,2))

iguais = lambda x,y: "SIM" if x == y else "NÃO"

print (iguais(3,4))

lista = [1,2,3,4,5,6]

elevar_quadrado = map(lambda x: x**2, lista)

print(list(elevar_quadrado))


valor = 5

mais_cinco = lambda x: x+5
    
assert mais_cinco(5) == 10

def somar (x,y) -> int:
    return x+y


def somar_passando(func, x,y) -> int:
    return func(x,y)

print(somar_passando(somar, 3,7))