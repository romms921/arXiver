import re

def test_regex():
    text = r'''
\institute{College, C025 New Science Center, 25 East Dr., Amherst, MA 01002-5000, USA
   \and
   Department of Astrophysical Sciences, Princeton University, 4 Ivy Lane, Princeton, NJ 08544, USA
   \and 
   Department of Physics, National and Kapodistrian University of Athens, University Campus Zografos, GR 15784, Athens, Greece
    \and
    Institute of Accelerating Systems \& Applications, University Campus Zografos, Athens, Greece
    \and
    
    Max-Planck-Institut f{\"u}r extraterrestrische Physik, Gie{\ss}enbachstra{\ss}e 1, D-85748 Garching, Germany
        \and
    Astronomical Observatory, University of Warsaw, Al. Ujazdowskie 4, 00-478, Warszawa, Poland
    \\
    }
'''
    # Current regex handles 2 levels of nesting
    pattern = r'\\(?:affiliation|institute)\{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}'
    match = re.search(pattern, text)
    
    if match:
        print("MATCH DETECTED:")
        content = match.group(1)
        print(content)
        # Check if the last part is present
        if "University of Warsaw" in content:
            print("\nSUCCESS: All content captured!")
        else:
            print("\nFAILURE: Content truncated!")
    else:
        print("NO MATCH FOUND")

if __name__ == "__main__":
    test_regex()
