def rewrite(axiom,rules,gen=3):
    s=axiom
    for _ in range(gen):
        s=''.join(''.join(rules.get(c,c))for c in s)
        print(s)
if __name__=='__main__':rewrite('A',{'A':'AB','B':'A'})