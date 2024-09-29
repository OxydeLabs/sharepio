#!/usr/bin/env python


if not __package__:
    import sys
    import os
    # Make CLI runnable from source tree with
    #    python src/spdl
    package_source_path = os.path.dirname(os.path.dirname(__file__))
    sys.path.insert(0, package_source_path)

if __name__ == '__main__':
    from .cli import main
    main()