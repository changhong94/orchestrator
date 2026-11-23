#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/signalfd.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

static int connect_to(const char *path,int type){
 struct sockaddr_un a={.sun_family=AF_UNIX};if(strlen(path)>=sizeof(a.sun_path)){errno=ENAMETOOLONG;return -1;}strcpy(a.sun_path,path);
 int fd=socket(AF_UNIX,type|SOCK_CLOEXEC,0);if(fd<0)return -1;
 if(connect(fd,(struct sockaddr*)&a,sizeof(a))<0){close(fd);return -1;}return fd;
}
static int send_fd(int sock,const void *data,size_t len,int fd){
 struct iovec iov={.iov_base=(void*)data,.iov_len=len};char ctrl[CMSG_SPACE(sizeof(int))]={0};struct msghdr m={.msg_iov=&iov,.msg_iovlen=1};
 if(fd>=0){m.msg_control=ctrl;m.msg_controllen=sizeof(ctrl);struct cmsghdr *c=CMSG_FIRSTHDR(&m);c->cmsg_level=SOL_SOCKET;c->cmsg_type=SCM_RIGHTS;c->cmsg_len=CMSG_LEN(sizeof(int));memcpy(CMSG_DATA(c),&fd,sizeof(fd));}
 return sendmsg(sock,&m,MSG_NOSIGNAL)==(ssize_t)len?0:-1;
}
int main(int argc,char **argv){
 if(argc!=6)return 2;
 int req=atoi(argv[3]),cg=atoi(argv[4]),lock=atoi(argv[5]);if(req<3||cg<3||lock<3||req==cg||req==lock||cg==lock)return 2;
 fcntl(req,F_SETFD,FD_CLOEXEC);fcntl(cg,F_SETFD,FD_CLOEXEC);fcntl(lock,F_SETFD,FD_CLOEXEC);
 sigset_t set;sigemptyset(&set);sigaddset(&set,SIGTERM);sigaddset(&set,SIGINT);if(sigprocmask(SIG_BLOCK,&set,NULL)<0)return 1;
 int sigfd=signalfd(-1,&set,SFD_CLOEXEC);if(sigfd<0)return 1;
 int c=connect_to(argv[1],SOCK_SEQPACKET);if(c<0){perror("shared controller connect");return 1;}
 struct ucred peer;socklen_t plen=sizeof(peer);if(getsockopt(c,SOL_SOCKET,SO_PEERCRED,&peer,&plen)<0||peer.uid!=0)return 1;
 char *raw=malloc(128*1024+1);if(!raw)return 1;ssize_t n=read(req,raw,128*1024+1);close(req);if(n<=0||n>128*1024)return 1;
 if(send_fd(c,raw,(size_t)n,cg)<0){return 1;}
 memset(raw,0,(size_t)n);free(raw);close(cg);
 struct pollfd p[2]={{.fd=c,.events=POLLIN},{.fd=sigfd,.events=POLLIN}};int canceled=0;
 for(;;){
  if(poll(p,2,-1)<0){if(errno==EINTR)continue;return 1;}
  if(p[1].revents&POLLIN){struct signalfd_siginfo info;if(read(sigfd,&info,sizeof(info))!=(ssize_t)sizeof(info))return 1;if(!canceled){if(send_fd(c,"C",1,-1)<0)return 1;canceled=1;}}
  if(p[0].revents&POLLIN){char b[8];n=recv(c,b,sizeof(b),0);if(n<=0)return 1;
   if(n==2&&b[0]=='X')return b[1]=='0'?0:1;
   if(n!=1||b[0]!='D')return 1;
   int fd=connect_to(argv[2],SOCK_STREAM);int rc=send_fd(c,fd<0?"E":"R",1,fd);if(fd>=0)close(fd);if(rc<0)return 1;
  }
  if(p[0].revents&(POLLHUP|POLLERR|POLLNVAL))return 1;
 }
}
