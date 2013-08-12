#!/usr/bin/perl

use strict;
use Time::Local;

sub str2time {
  my $str = $_[0];
  if ($str =~ /^(\d+)-(\d+)-(\d+)\s+(\d+):(\d+):(\d+)/) {
    return timegm($6,$5,$4,$3,$2,$1);
  }
  return 0;
}

my %id_to_send_time;
my %id_to_bolt_time;
my %id_to_ack_time;
my %id_to_done_time;

my %timed_out_ack_ids;
my %failed_ack_ids;
my %success_ack_ids;
my %all_ack_ids;
my %ack_id_to_min_send_time;

my %sent_ids;
my %recv_ids;

my %task_to_name;

while (<>) {
  #The order of the lines is not time order because the logs are not sorted so a receive can happen before a send
  my $line = $_;
  #TODO Acks have no ID, so if we want to track the acks we need a different RE 
  if ($line =~ /^(\d+-\d+-\d+\s+\d+:\d+:\d+).*TRANSFERING tuple TASK: (\d+) TUPLE: source: ([^\s:]+):(\d+), stream: (\S+), id: \{(-?\d+)=(-?\d+)\}, (.*)$/) {
    my $time_str = $1;
    my $time = str2time($time_str);
    my $to_task = $2;
    my $source_name = $3;
    my $source_task = $4;
    my $stream = $5;
    my $ack_id = $6;
    my $tuple_id = $7;
    my $full_id = "$ack_id:$tuple_id";
    my $data = $8;

    $all_ack_ids{$ack_id} = 1;

    if (defined $recv_ids{$full_id}) {
      delete $recv_ids{$full_id};
    } else {
      $sent_ids{$full_id} = 1;
    }

    $id_to_send_time{$full_id} = $time;
    if  (! defined $ack_id_to_min_send_time{$ack_id}) {
      $ack_id_to_min_send_time{$ack_id} = $time;
    } else {
      my $tmp = $ack_id_to_min_send_time{$ack_id};
      if($tmp > $time) {
        $ack_id_to_min_send_time{$ack_id} = $time;
      } else {
        $ack_id_to_min_send_time{$ack_id} = $tmp;
      }
    }

    $task_to_name{$source_task} = $source_name;

    my $op = "SEND";
    print "$time\t$op\t$source_task\t$ack_id\t$tuple_id\t$to_task\t$data\n";
  } elsif ($line =~ /^(\d+-\d+-\d+\s+\d+:\d+:\d+).*Processing received message FOR (\d*) TUPLE: source: ([^\s:]+):(\d+), stream: (\S+), id: \{(-?\d+)=(-?\d+)\}, (.*)$/) {
    #Worker has gotten the tuple
    my $time_str = $1;
    my $time = str2time($time_str);
    my $to_task = $2;
    my $source_name = $3;
    my $source_task = $4;
    my $stream = $5;
    my $ack_id = $6;
    my $tuple_id = $7;
    my $full_id = "$ack_id:$tuple_id";
    my $data = $8;

    if (defined $sent_ids{$full_id}) {
      delete $sent_ids{$full_id};
    } else {
      $recv_ids{$full_id} = 1;
    }

    my $op = "RECV";
    print "$time\t$op\t$source_task\t$ack_id\t$tuple_id\t$to_task\t$data\n";
  } elsif ($line =~ /^(\d+-\d+-\d+\s+\d+:\d+:\d+).*Received tuple source: ([^\s:]+):(\d+), stream: (\S+), id: \{(-?\d+)=(-?\d+)\}, (.*) at task (\d+)/) {
    #BOLT is about to start processing
    my $time_str = $1;
    my $time = str2time($time_str);
    my $source_name = $2;
    my $source_task = $3;
    my $stream = $4;
    my $ack_id = $5;
    my $tuple_id = $6;
    my $full_id = "$ack_id:$tuple_id";
    my $data = $7;
    my $to_task = $8;

    $id_to_bolt_time{$full_id} = $time;

    my $op = "BOLT";
    print "$time\t$op\t$source_task\t$ack_id\t$tuple_id\t$to_task\t$data\n";
  } elsif ($line =~ /^(\d+-\d+-\d+\s+\d+:\d+:\d+).*BOLT ack TASK: (\d+) TIME: \d* TUPLE: source: ([^\s:]+):(\d+), stream: (\S+), id: \{(-?\d+)=(-?\d+)\}, (.*)/) {
    #BOLT acks the tuple
    my $time_str = $1;
    my $time = str2time($time_str);
    my $to_task = $2;
    my $source_name = $3;
    my $source_task = $4;
    my $stream = $5;
    my $ack_id = $6;
    my $tuple_id = $7;
    my $full_id = "$ack_id:$tuple_id";
    my $data = $8;

    $id_to_ack_time{$full_id} = $time;

    my $op = "B_ACK";
    print "$time\t$op\t$source_task\t$ack_id\t$tuple_id\t$to_task\t$data\n";
  } elsif ($line =~ /^(\d+-\d+-\d+\s+\d+:\d+:\d+).*Execute done TUPLE source: ([^\s:]+):(\d+), stream: (\S+), id: \{(-?\d+)=(-?\d+)\}, (.*) TASK: (\d+) DELTA: \d*/) {
    #BOLT exits processing
    my $time_str = $1;
    my $time = str2time($time_str);
    my $source_name = $2;
    my $source_task = $3;
    my $stream = $4;
    my $ack_id = $5;
    my $tuple_id = $6;
    my $full_id = "$ack_id:$tuple_id";
    my $data = $7;
    my $to_task = $8;

    $id_to_done_time{$full_id} = $time;

    my $op = "B_DONE";
    print "$time\t$op\t$source_task\t$ack_id\t$tuple_id\t$to_task\t$data\n";
  } elsif ($line =~ /^(\d+-\d+-\d+\s+\d+:\d+:\d+).*SPOUT Failing (-?\d+): .* REASON: (\S+) .*/) {
    #SPOUT_FAILS
    my $time_str = $1;
    my $time = str2time($time_str);
    my $ack_id = $2;
    my $reason = $3;
    my $source_task = "";
    my $tuple_id = "";
    my $to_task = "";
    my $data = "";

    if ($reason eq "TIMEOUT") {
      $timed_out_ack_ids{$ack_id} = 1;
    } else {
      $failed_ack_ids{$ack_id} = 1;
    }

    my $op = "SPOUT_FAIL ". $reason;
    print "$time\t$op\t$source_task\t$ack_id\t$tuple_id\t$to_task\t$data\n";
  } elsif ($line =~ /^(\d+-\d+-\d+\s+\d+:\d+:\d+).*SPOUT Acking message (-?\d+) .*/) {
    #SPOUT_ACK
    my $time_str = $1;
    my $time = str2time($time_str);
    my $ack_id = $2;
    my $source_task = "";
    my $tuple_id = "";
    my $to_task = "";
    my $data = "";

    $success_ack_ids{$ack_id} = 1;

    my $op = "SPOUT_ACK";
    print "$time\t$op\t$source_task\t$ack_id\t$tuple_id\t$to_task\t$data\n";
  } 
}

print "\n\nTASK_MAP:\n";
foreach my $key (sort {(int $a) - (int $b)} keys(%task_to_name)) {
  print "\t$key -> $task_to_name{$key}\n";
}

print "\nSUCCESS: ".scalar(keys(%success_ack_ids))."\n";
print "TIMED-OUT: ".scalar(keys(%timed_out_ack_ids))."\n";
print "FAILED: ".scalar(keys(%failed_ack_ids))."\n";
print "KNOWN: ".scalar(keys(%all_ack_ids))."\n";

print "\n\nLOST IN TRANSFER:\n";
my @allIds;
push @allIds, keys(%sent_ids);
push @allIds, keys(%recv_ids);
foreach my $id (sort @allIds) {
  print "$id\n";
}

print "\n\nTIMED OUT:\n";
foreach my $id (keys(%timed_out_ack_ids)) {
  my $first_send = $ack_id_to_min_send_time{$id};
  print "$id\t$first_send\n";
  my @all_ids = grep(/^$id:/, keys(%id_to_send_time));
  foreach my $full_id (sort {$id_to_send_time{$a} cmp $id_to_send_time{$b}} @all_ids) {
    my $send = $id_to_send_time{$full_id};
    my $bolt = $id_to_bolt_time{$full_id};
    my $ack = $id_to_ack_time{$full_id};
    my $done = $id_to_done_time{$full_id};
    print "$full_id\t$send\t".($send-$first_send)."\t$bolt\t".($bolt-$first_send)."\t$ack\t".($ack-$first_send)."\t$done\t".($done-$first_send)."\n";
  }
}

print "\n\nLOST:\n";
foreach my $id (keys(%all_ack_ids)) {
  if (defined $success_ack_ids{$id} or
      defined $timed_out_ack_ids{$id} or
      defined $failed_ack_ids{$id}) {
    next;
  }
  my $first_send = $ack_id_to_min_send_time{$id};
  print "$id\t$first_send\n";
  my @all_ids = grep(/^$id:/, keys(%id_to_send_time));
  foreach my $full_id (sort {$id_to_send_time{$a} cmp $id_to_send_time{$b}} @all_ids) {
    my $send = $id_to_send_time{$full_id};
    my $bolt = $id_to_bolt_time{$full_id};
    my $ack = $id_to_ack_time{$full_id};
    my $done = $id_to_done_time{$full_id};
    print "$full_id\t$send\t".($send-$first_send)."\t$bolt\t".($bolt-$first_send)."\t$ack\t".($ack-$first_send)."\t$done\t".($done-$first_send)."\n";
  }
}


