#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/in.h>
#include <linux/ip.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#ifndef CANDIDATE_MARK
#define CANDIDATE_MARK 1
#endif

#ifndef ETH_P_IP
#define ETH_P_IP 0x0800
#endif

struct ethhdr {
	__u8 h_dest[6];
	__u8 h_source[6];
	__be16 h_proto;
} __attribute__((packed));

struct udphdr {
	__be16 source;
	__be16 dest;
	__be16 len;
	__be16 check;
} __attribute__((packed));

SEC("tc")
int udp_egress(struct __sk_buff *skb)
{
    void *data = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;
    struct ethhdr *eth = data;
    struct iphdr *ip;
    struct udphdr *udp;

    if ((void *)(eth + 1) > data_end) {
        return TC_ACT_OK;
    }

    if (eth->h_proto != bpf_htons(ETH_P_IP)) {
        return TC_ACT_OK;
    }

    ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end) {
        return TC_ACT_OK;
    }

    if (ip->ihl < 5) {
        return TC_ACT_OK;
    }

    if (ip->protocol != IPPROTO_UDP) {
        return TC_ACT_OK;
    }

    udp = (void *)ip + (ip->ihl * 4);
    if ((void *)(udp + 1) > data_end) {
        return TC_ACT_OK;
    }

    skb->mark = CANDIDATE_MARK;
    return TC_ACT_OK;
}

char LICENSE[] SEC("license") = "GPL";
