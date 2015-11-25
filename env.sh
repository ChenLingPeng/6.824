# run this script using 'source $0'
filepath=$(cd "$(dirname "$0")"; pwd)
echo $filepath will be add to $GOPATH for now
export GOPATH=$GOPATH:$filepath
echo now GOPATH is $GOPATH
