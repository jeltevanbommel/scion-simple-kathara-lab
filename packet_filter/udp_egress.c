#include <linux/bpf.h>
#include <linux/in.h>
#include <linux/ip.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#ifndef CANDIDATE_MARK
#define CANDIDATE_MARK 1
#endif

struct udphdr {
	__be16 source;
	__be16 dest;
	__be16 len;
	__be16 check;
} __attribute__((packed));

SEC("cgroup/skb")
int udp_egress(struct __sk_buff *skb)
{
    void *data = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;
    struct iphdr *ip;
    struct udphdr *udp;
    __u16 src_port = (__u16)skb->local_port;

    if (src_port != 0) {
        src_port = bpf_ntohs(src_port);
        if (src_port >= 6000) {
            return 1;
        }
        skb->mark = CANDIDATE_MARK;
        return 1;
    }

    ip = data;
    if ((void *)(ip + 1) > data_end) {
        return 1;
    }

    if (ip->ihl < 5) {
        return 1;
    }

    if (ip->protocol != IPPROTO_UDP) {
        return 1;
    }

    udp = (void *)ip + (ip->ihl * 4);
    if ((void *)(udp + 1) > data_end) {
        return 1;
    }

    if (udp->source >= bpf_htons(6000)) {
        return 1;
    }

    skb->mark = CANDIDATE_MARK;
    return 1;
}

char LICENSE[] SEC("license") = "GPL";
