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

#ifndef ICMP_ECHO
#define ICMP_ECHO 8
#endif

#ifndef ICMP_ECHOREPLY
#define ICMP_ECHOREPLY 0
#endif

struct icmphdr {
	__u8 type;
	__u8 code;
	__be16 checksum;
	__be16 un_id;
	__be16 un_sequence;
} __attribute__((packed));

SEC("tc")
int icmp_egress(struct __sk_buff *skb)
{
    void *data = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;
    struct ethhdr *eth = data;
    struct iphdr *ip;
    struct icmphdr *icmp;

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

    if (ip->protocol != IPPROTO_ICMP) {
        return TC_ACT_OK;
    }

    icmp = (void *)ip + (ip->ihl * 4);
    if ((void *)(icmp + 1) > data_end) {
        return TC_ACT_OK;
    }

    if (icmp->type == ICMP_ECHO) {
        skb->mark = CANDIDATE_MARK;
    }

    return TC_ACT_OK;
}

char LICENSE[] SEC("license") = "GPL";
