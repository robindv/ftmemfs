# ftmemfs
This readme explains shortly how to compile ftmemfs and how to use it.
We assume that a copy of this ftmemfs folder is in your (shared) home directory.

## Preparation

- Create a `bin`, `ftstate`,  `lib` and `include` directory in your home-directory.
- Add the following lines to your .bashrc:

```sh
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/lib/
export LIBRARY_PATH=$LD_LIBRARY_PATH
export C_INCLUDE_PATH=$HOME/include
export PATH=$PATH:$HOME/bin
```

## Dependencies

### Redis
Download redis server, in our case, redis-3.0.7 and copy the binaries in your `~/bin` directory.
```sh
cd redis-3.0.7
make
cp src/redis-cli ~/bin
cp src/redis-server ~/bin
```

## Compilation of the filesystem

### Zookeeper
Download the latest stable zookeeper release (currently 3.4.9), go to `/src/c/` and:
```sh
cd zookeeper-3.4.9/src/c/
./configure --prefix=$HOME
make
make install
```
### Hiredis
Clone the latest version from the github repository
```sh
git clone https://github.com/redis/hiredis.git
cd hiredis
make
cp *.h ~/include
cp libhiredis.so ~/lib
```

### Eredis (optional)
Clone the latest version of eredis, and compile it. This might require libev,
if it is not already present on your system, it can be found here: https://github.com/enki/libev
```sh
git clone https://github.com/EulerianTechnologies/eredis.git
cd eredis
cmake -DCMAKE_INSTALL_PREFIX=$HOME .
make
make install
```

#### libev
```sh
git clone https://github.com/enki/libev.git
cd libev
./configure --prefix=$HOME
make
make install
```

### Compiling ftmemfs
Now, you can enter the fuse directory and compile the filesystem. Optionally you can enable eredis or debug modus by modifying the Makefile.
```sh
cd $HOME/ftmemfs/fuse
make
```

## Configuration
Create an virtualenv, and install the requirements:
```sh
cd $HOME
virtualenv ftenv
ftenv/bin/pip install -r ftmemfs/requirements.txt
```
Please now review `config.ini`, and the shell-scripts in the `scripts` directory. You have to create a file named `config_platform.ini` for the platform specific information. You can also create a symlink using one of the existing files, for example on DAS-5:
```sh
cd ~/ftmemfs
ln -s config_das5.ini config_platform.ini
```

## Starting ftmemfs
Simply execute:
```sh
ftmemfs/ftcli.sh
```
Please review `ftcli.py` to see what commands are available. Use the `services` command to review the roles of your nodes.

You can also start a localmanager on a server manually by executing
```sh
~/ftmemfs/scripts/start_locally.sh
```

### Final notes
The code contains `todo` remarks everywhere, with all kind of hints on how it could be improved. Although the code essentially works, it is by no means completely polished/finished and only interesting when you have the time to debug.
